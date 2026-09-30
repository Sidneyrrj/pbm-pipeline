"""
SILVER layer ingestion (medallion architecture) for pbm-pipeline.

Reads the bronze Delta tables, validates every record and splits the result:

    medallion/bronze_delta/<schema>/<table>
        ├── valid records    -> medallion/silver_delta/<schema>/<table>
        └── invalid records  -> medallion/quarantine_delta/<schema>/<table>

Validations applied:
  * string    : not null, of type string and not empty (after strip)
  * float     : not null, numeric and finite
  * timestamp : not null, valid date/time in the YYYY-MM-DD HH:MM:SS format (time kept)
  * key       : claims/reverts -> ``id``; pharmacies -> (``npi``, ``chain``), since the
                same npi may belong to several chains. Duplicated key within a batch:
                claims/reverts keep the VALID event with the most recent timestamp,
                pharmacies keep the version ingested last; the rest goes to quarantine
  * claims    : ``npi`` must exist in the ``pharmacies`` silver table
  * business  : ``quantity`` must be greater than zero

Quarantine:
  * Same schema as bronze (original values kept, full timestamp)
    + ``_rejection_reasons`` (e.g. "quantity:null;npi:not_in_pharmacies")
    + ``_quarantined_at``.

Modes:
  * incremental (default): processes only bronze records whose ``_ingested_at``
    is greater than the last one already processed (highest ``_ingested_at``
    found in silver or quarantine).
      - silver     : MERGE (upsert) on the primary key
                     pharmacies (npi + chain) -> new pair is inserted, existing pair updated
                     claims/reverts (id) -> the event with the most recent timestamp wins
                     (only records that passed every validation reach the MERGE)
      - quarantine : invalid records of the batch are APPENDED
      - FK         : ``npi`` is checked against the COMPLETE pharmacies silver table
  * overwrite: reprocesses the whole bronze layer and rewrites silver and quarantine.
    Use it after a ``bronze_ingestion.py --mode overwrite`` or to re-evaluate
    quarantined records (e.g. a pharmacy that arrived later).

Usage:
    python silver_ingestion.py                    # incremental
    python silver_ingestion.py --mode overwrite   # reprocess everything
"""

from __future__ import annotations

import argparse
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent
MEDALLION_DIR = PROJECT_ROOT / "medallion"
BRONZE_DIR = MEDALLION_DIR / "bronze_delta"
SILVER_DIR = MEDALLION_DIR / "silver_delta"
QUARANTINE_DIR = MEDALLION_DIR / "quarantine_delta"

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMESTAMP_REGEX = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"

STRING, FLOAT, TIMESTAMP = "string", "float", "timestamp"

# Arrow types per logical type: (silver, quarantine/bronze)
ARROW_TYPES = {
    STRING: (pa.string(), pa.string()),
    FLOAT: (pa.float64(), pa.float64()),
    TIMESTAMP: (pa.timestamp("us"), pa.timestamp("us")),
}

BRONZE_LINEAGE = [
    pa.field("_source_file", pa.string()),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("silver")


@dataclass(frozen=True)
class TableSpec:
    schema_name: str
    table_name: str
    fields: dict[str, str]                 # column -> logical type
    primary_key: tuple[str, ...]           # one or more columns
    order_by: str | None = None            # column used to break ties between duplicates
    keep: str = "first"                    # "first" = lowest order_by wins | "last" = highest
    foreign_keys: list[tuple[str, str, str]] = field(default_factory=list)
    # (local_column, referenced_table, referenced_column)
    business_rules: list[tuple[str, str, Callable[[pd.Series], pd.Series]]] = field(default_factory=list)
    # (column, rule_name, function returning True for VALID values)
    merge_update_predicate: str | None = None
    # condition for the MERGE to update an existing record (None = always update)

    def path(self, layer_dir: Path) -> Path:
        return layer_dir / self.schema_name / self.table_name

    def silver_schema(self) -> pa.Schema:
        cols = [pa.field(c, ARROW_TYPES[t][0], nullable=False) for c, t in self.fields.items()]
        return pa.schema(cols + BRONZE_LINEAGE + [
            pa.field("_processed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ])

    def quarantine_schema(self) -> pa.Schema:
        cols = [pa.field(c, ARROW_TYPES[t][1]) for c, t in self.fields.items()]
        return pa.schema(cols + BRONZE_LINEAGE + [
            pa.field("_rejection_reasons", pa.string(), nullable=False),
            pa.field("_quarantined_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ])


# Order matters: pharmacies must be in silver before claims are validated.
TABLES = [
    TableSpec(
        schema_name="pharmacy",
        table_name="pharmacies",
        fields={"npi": STRING, "chain": STRING},
        primary_key=("npi", "chain"),
        order_by="_ingested_at",
        keep="last",                         # most recent version of the pharmacy wins
    ),
    TableSpec(
        schema_name="claim_event",
        table_name="claims",
        fields={"id": STRING, "npi": STRING, "ndc": STRING,
                "price": FLOAT, "quantity": FLOAT, "timestamp": TIMESTAMP},
        primary_key=("id",),
        order_by="timestamp",
        keep="last",                         # most recent valid event wins
        merge_update_predicate='s."timestamp" > t."timestamp"',
        foreign_keys=[("npi", "pharmacies", "npi")],
        business_rules=[("quantity", "not_positive", lambda s: s > 0)],
    ),
    TableSpec(
        schema_name="revert_event",
        table_name="reverts",
        fields={"id": STRING, "claim_id": STRING, "timestamp": TIMESTAMP},
        primary_key=("id",),
        order_by="timestamp",
        keep="last",                         # most recent valid event wins
        merge_update_predicate='s."timestamp" > t."timestamp"',
    ),
]


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
def table_exists(path: Path) -> bool:
    return DeltaTable.is_deltatable(str(path))


def read_column(path: Path, column: str) -> pd.Series:
    """Reads a single column from a Delta table (empty if the table does not exist)."""
    if not table_exists(path):
        return pd.Series(dtype="object")
    return DeltaTable(str(path)).to_pandas(columns=[column])[column]


def last_processed_at(spec: TableSpec) -> pd.Timestamp | None:
    """Highest ``_ingested_at`` already processed (silver + quarantine)."""
    values = pd.concat([
        read_column(spec.path(SILVER_DIR), "_ingested_at"),
        read_column(spec.path(QUARANTINE_DIR), "_ingested_at"),
    ]).dropna()
    return pd.Timestamp(values.max()) if len(values) else None


def reference_values(table_name: str, column: str) -> set:
    """Values of the column in the COMPLETE silver table being referenced."""
    spec = next(t for t in TABLES if t.table_name == table_name)
    return set(read_column(spec.path(SILVER_DIR), column).dropna())


def read_bronze(spec: TableSpec, since: pd.Timestamp | None = None) -> pd.DataFrame:
    path = spec.path(BRONZE_DIR)
    if not table_exists(path):
        raise FileNotFoundError(f"Bronze table not found: {path}. Run bronze_ingestion.py first.")
    df = DeltaTable(str(path)).to_pandas()
    if since is not None:
        df = df[pd.to_datetime(df["_ingested_at"], utc=True) > since]
    missing = set(spec.fields) - set(df.columns)
    for col in missing:
        log.warning("[%s] column '%s' missing in bronze -> treated as null", spec.table_name, col)
        df[col] = None
    for col in ("_source_file", "_ingested_at"):
        if col not in df.columns:
            df[col] = None
    return df


# --------------------------------------------------------------------------- #
# Type validations (return {reason: boolean mask of invalid rows})
# --------------------------------------------------------------------------- #
def _is_null(s: pd.Series) -> pd.Series:
    return s.isna()


def validate_string(s: pd.Series) -> tuple[dict[str, pd.Series], pd.Series]:
    null = _is_null(s)
    not_string = ~null & ~s.map(lambda v: isinstance(v, str))
    cleaned = s.where(null | not_string, s.astype("string").str.strip())
    empty = ~null & ~not_string & (cleaned == "")
    return {"null": null, "not_string": not_string, "empty": empty.fillna(False)}, cleaned


def validate_float(s: pd.Series) -> tuple[dict[str, pd.Series], pd.Series]:
    null = _is_null(s)

    def _bad_number(v) -> bool:
        if isinstance(v, bool):
            return True
        try:
            return not math.isfinite(float(v))
        except (TypeError, ValueError):
            return True

    not_float = ~null & s.map(_bad_number)
    cleaned = pd.to_numeric(s.where(~not_float), errors="coerce").astype("float64")
    return {"null": null, "not_float": not_float}, cleaned


def validate_timestamp(s: pd.Series) -> tuple[dict[str, pd.Series], pd.Series]:
    null = _is_null(s)
    parsed = pd.to_datetime(s, errors="coerce", format="mixed").dt.floor("s")
    formatted = parsed.dt.strftime(TIMESTAMP_FORMAT)
    invalid = ~null & (parsed.isna() | ~formatted.fillna("").str.match(TIMESTAMP_REGEX))
    return {"null": null, "invalid_timestamp": invalid}, parsed   # YYYY-MM-DD HH:MM:SS


VALIDATORS = {STRING: validate_string, FLOAT: validate_float, TIMESTAMP: validate_timestamp}


# --------------------------------------------------------------------------- #
# Rules
# --------------------------------------------------------------------------- #
def add_reason(reasons: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    mask = pd.Series(mask, index=reasons.index).fillna(False).astype(bool)
    return reasons.where(~mask, reasons.where(reasons == "", reasons + ";") + reason)


def apply_rules(df: pd.DataFrame, spec: TableSpec,
                ref_lookup: Callable[[str, str], set] = reference_values
                ) -> tuple[pd.DataFrame, pd.Series]:
    """Returns (df with cleaned columns, series of reasons; "" = valid record)."""
    df = df.copy().reset_index(drop=True)
    reasons = pd.Series("", index=df.index, dtype="object")

    # 1. type / null / empty
    for col, kind in spec.fields.items():
        checks, cleaned = VALIDATORS[kind](df[col])
        df[col] = cleaned
        for reason, mask in checks.items():
            reasons = add_reason(reasons, mask, f"{col}:{reason}")

    # 2. referential integrity
    for col, ref_table, ref_col in spec.foreign_keys:
        ref_values = ref_lookup(ref_table, ref_col)
        orphan = (reasons == "") & ~df[col].isin(ref_values)
        reasons = add_reason(reasons, orphan, f"{col}:not_in_{ref_table}")

    # 3. business rules (evaluated only where the value passed the type validation)
    for col, rule_name, is_valid in spec.business_rules:
        broken = df[col].notna() & ~is_valid(df[col]).fillna(False).astype(bool)
        reasons = add_reason(reasons, broken, f"{col}:{rule_name}")

    # 4. duplicated primary key (among the records still valid at this point)
    valid = reasons == ""
    if valid.any():
        candidates = df[valid]
        if spec.order_by:
            candidates = candidates.sort_values(spec.order_by, kind="stable")
        dup_idx = candidates.index[candidates.duplicated(subset=list(spec.primary_key), keep=spec.keep)]
        reasons = add_reason(reasons, df.index.isin(dup_idx), f"{'+'.join(spec.primary_key)}:duplicate")

    return df, reasons


def build_silver(df: pd.DataFrame, reasons: pd.Series, spec: TableSpec, now: pd.Timestamp) -> pd.DataFrame:
    out = df.loc[reasons == "", list(spec.fields) + ["_source_file", "_ingested_at"]].copy()
    out["_processed_at"] = now
    return out


def build_quarantine(bronze: pd.DataFrame, reasons: pd.Series, spec: TableSpec,
                     now: pd.Timestamp) -> pd.DataFrame:
    """Invalid records with the ORIGINAL bronze values."""
    bad = reasons != ""
    cols = list(spec.fields) + ["_source_file", "_ingested_at"]
    out = bronze.reset_index(drop=True).loc[bad, cols].copy()
    for col, kind in spec.fields.items():   # ensures types compatible with the schema
        if kind == STRING:
            out[col] = out[col].map(lambda v: v if isinstance(v, str) or pd.isna(v) else str(v))
        elif kind == FLOAT:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
        else:
            out[col] = pd.to_datetime(out[col], errors="coerce", format="mixed")
    out["_rejection_reasons"] = reasons[bad].values
    out["_quarantined_at"] = now
    return out


# --------------------------------------------------------------------------- #
# Delta writing
# --------------------------------------------------------------------------- #
def write_delta(df: pd.DataFrame, schema: pa.Schema, path: Path, mode: str) -> None:
    """mode: overwrite | append."""
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    if mode == "overwrite":
        write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")
    else:
        write_deltalake(str(path), table, mode="append")
    log.info("  %d row(s) -> %s (%s)", table.num_rows, path, mode)


def merge_silver(df: pd.DataFrame, spec: TableSpec) -> None:
    """Upsert into silver on the primary key (creates the table if it does not exist)."""
    path = spec.path(SILVER_DIR)
    if not table_exists(path):
        write_delta(df, spec.silver_schema(), path, "append")
        return
    source = pa.Table.from_pandas(df, schema=spec.silver_schema(), preserve_index=False)
    predicate = " AND ".join(f't."{c}" = s."{c}"' for c in spec.primary_key)
    metrics = (
        DeltaTable(str(path))
        .merge(source=source, predicate=predicate,
               source_alias="s", target_alias="t")
        .when_matched_update_all(predicate=spec.merge_update_predicate)
        .when_not_matched_insert_all()
        .execute()
    )
    log.info("  MERGE -> %s | inserted=%s updated=%s", path,
             metrics.get("num_target_rows_inserted"), metrics.get("num_target_rows_updated"))


def quarantine_records(df: pd.DataFrame, spec: TableSpec, mode: str = "append") -> None:
    """Writes the invalid records to the isolated quarantine table."""
    write_delta(df, spec.quarantine_schema(), spec.path(QUARANTINE_DIR), mode)


def summarize(reasons: pd.Series, spec: TableSpec) -> None:
    bad = reasons[reasons != ""]
    log.info("[%s] %d valid | %d quarantined", spec.table_name, (reasons == "").sum(), len(bad))
    if len(bad):
        counts = bad.str.split(";").explode().value_counts()
        for reason, n in counts.items():
            log.info("    %-28s %d", reason, n)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(mode: str) -> dict[str, pd.DataFrame]:
    """Processes the tables in order (pharmacies before claims because of the FK)."""
    now = pd.Timestamp(datetime.now(timezone.utc))
    batches: dict[str, pd.DataFrame] = {}

    for spec in TABLES:
        log.info("Processing %s/%s (%s)", spec.schema_name, spec.table_name, mode)
        since = last_processed_at(spec) if mode == "incremental" else None
        bronze = read_bronze(spec, since)
        if since is not None:
            log.info("[%s] %d new record(s) in bronze since %s",
                     spec.table_name, len(bronze), since)
        if mode == "incremental" and bronze.empty:
            log.info("[%s] nothing new to process", spec.table_name)
            continue

        cleaned, reasons = apply_rules(bronze, spec)
        summarize(reasons, spec)
        silver = build_silver(cleaned, reasons, spec, now)
        quarantine = build_quarantine(bronze, reasons, spec, now)

        if mode == "overwrite":
            write_delta(silver, spec.silver_schema(), spec.path(SILVER_DIR), "overwrite")
            quarantine_records(quarantine, spec, "overwrite")
        else:
            if not silver.empty:
                merge_silver(silver, spec)
            if not quarantine.empty:
                quarantine_records(quarantine, spec, "append")

        batches[spec.table_name] = silver
    return batches


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver ingestion + quarantine -> Delta Lake")
    parser.add_argument("--mode", choices=["incremental", "overwrite"], default="incremental")
    run(parser.parse_args().mode)


if __name__ == "__main__":
    main()
