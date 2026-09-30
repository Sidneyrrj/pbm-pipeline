"""
BRONZE layer ingestion (medallion architecture) for pbm-pipeline.

Reads the raw files from ``data/`` and writes each dataset as a Delta table:

    data/pharmacies/*.csv  ->  medallion/bronze_delta/pharmacy/pharmacies
    data/claims/*.json     ->  medallion/bronze_delta/claim_event/claims
    data/reverts/*.json    ->  medallion/bronze_delta/revert_event/reverts

Bronze layer rules:
  * Data is kept as it arrived (no deduplication or filtering);
    only type casting according to the schema.
  * Lineage columns: ``_source_file`` and ``_ingested_at``.
  * macOS AppleDouble files (``._*``) are ignored.
  * Invalid JSON files are logged and skipped.

Modes:
  * incremental (default): ingests only files not yet present in the table
    (compared by file name in ``_source_file``) and APPENDS them.
  * overwrite: re-reads every file in data/ and rewrites the tables.

Usage:
    pip install pandas pyarrow deltalake
    python bronze_ingestion.py                    # incremental
    python bronze_ingestion.py --mode overwrite   # reprocess everything
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = PROJECT_ROOT / "medallion" / "bronze_delta"

LINEAGE_FIELDS = [
    pa.field("_source_file", pa.string(), nullable=False),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bronze")


@dataclass(frozen=True)
class Dataset:
    schema_name: str      # schema folder inside bronze_delta
    table_name: str       # table name = folder name in data/
    file_format: str      # "csv" | "json"
    schema: pa.Schema     # data schema (without lineage columns)

    @property
    def source_dir(self) -> Path:
        return DATA_DIR / self.table_name

    @property
    def target_dir(self) -> Path:
        return BRONZE_DIR / self.schema_name / self.table_name

    @property
    def full_schema(self) -> pa.Schema:
        return pa.schema(list(self.schema) + LINEAGE_FIELDS)


DATASETS = [
    Dataset(
        schema_name="pharmacy",
        table_name="pharmacies",
        file_format="csv",
        schema=pa.schema([
            pa.field("npi", pa.string()),     # pharmacy identifier
            pa.field("chain", pa.string()),   # chain the pharmacy belongs to
        ]),
    ),
    Dataset(
        schema_name="claim_event",
        table_name="claims",
        file_format="json",
        schema=pa.schema([
            pa.field("id", pa.string()),         # claim UUID
            pa.field("npi", pa.string()),        # pharmacy that filled the claim
            pa.field("ndc", pa.string()),        # drug identifier
            pa.field("price", pa.float64()),     # unit_price * quantity
            pa.field("quantity", pa.float64()),  # integer/float -> float64
            pa.field("timestamp", pa.timestamp("us")),
        ]),
    ),
    Dataset(
        schema_name="revert_event",
        table_name="reverts",
        file_format="json",
        schema=pa.schema([
            pa.field("id", pa.string()),         # revert UUID
            pa.field("claim_id", pa.string()),   # claim being invalidated
            pa.field("timestamp", pa.timestamp("us")),
        ]),
    ),
]


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
def list_source_files(ds: Dataset) -> list[Path]:
    # modification order (oldest first): row order reflects file age
    files = sorted(
        (p for p in ds.source_dir.glob(f"*.{ds.file_format}")
         if p.is_file() and not p.name.startswith("._")),
        key=lambda p: (p.stat().st_mtime, p.name),
    )
    if not files:
        log.warning("No .%s files found in %s", ds.file_format, ds.source_dir)
    return files


def already_ingested_files(ds: Dataset) -> set[str]:
    """File names already present in the bronze table (empty if it does not exist)."""
    path = str(ds.target_dir)
    if not DeltaTable.is_deltatable(path):
        return set()
    df = DeltaTable(path).to_pandas(columns=["_source_file"])
    return set(df["_source_file"].dropna().unique())


def select_files(ds: Dataset, mode: str) -> list[Path]:
    files = list_source_files(ds)
    if mode == "overwrite":
        return files
    done = already_ingested_files(ds)
    new_files = [f for f in files if f.name not in done]
    log.info("[%s] %d file(s) in folder | %d already ingested | %d new",
             ds.table_name, len(files), len(files) - len(new_files), len(new_files))
    return new_files


def read_csv_file(path: Path) -> pd.DataFrame:
    # dtype=str keeps leading zeros (e.g. npi "0123456789")
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def read_json_file(path: Path) -> pd.DataFrame:
    with path.open(encoding="utf-8") as fh:
        payload = json.load(fh)
    if isinstance(payload, dict):
        payload = [payload]
    # json.load keeps strings as strings (ndc/npi keep their leading zeros)
    return pd.DataFrame.from_records(payload)


def read_dataset(ds: Dataset, files: list[Path]) -> pd.DataFrame:
    reader = read_csv_file if ds.file_format == "csv" else read_json_file
    frames: list[pd.DataFrame] = []

    for path in files:
        try:
            df = reader(path)
        except (json.JSONDecodeError, UnicodeDecodeError, pd.errors.ParserError) as exc:
            log.error("Skipped file (invalid content) %s: %s", path.name, exc)
            continue
        df["_source_file"] = path.name
        frames.append(df)

    columns = ds.schema.names + ["_source_file"]
    if not frames:
        return pd.DataFrame(columns=columns)

    df = pd.concat(frames, ignore_index=True)

    unexpected = set(df.columns) - set(columns)
    if unexpected:
        log.warning("[%s] columns outside the schema dropped: %s", ds.table_name, sorted(unexpected))
    # ensures every schema column exists (missing ones become null) and the column order
    return df.reindex(columns=columns)


# --------------------------------------------------------------------------- #
# Typing
# --------------------------------------------------------------------------- #
def cast_to_schema(df: pd.DataFrame, ds: Dataset) -> pd.DataFrame:
    df = df.copy()
    for field in ds.schema:
        col = field.name
        before_nulls = df[col].isna().sum()

        if pa.types.is_string(field.type):
            df[col] = df[col].astype("string")
        elif pa.types.is_floating(field.type):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        elif pa.types.is_timestamp(field.type):
            df[col] = pd.to_datetime(df[col], errors="coerce", format="ISO8601")
        else:
            raise TypeError(f"Unhandled type: {field.type}")

        new_nulls = df[col].isna().sum() - before_nulls
        if new_nulls:
            log.warning("[%s] %d value(s) of '%s' could not be cast to %s -> null",
                        ds.table_name, new_nulls, col, field.type)
        if before_nulls:
            log.info("[%s] %d record(s) missing '%s' at the source", ds.table_name, before_nulls, col)

    df["_ingested_at"] = pd.Timestamp(datetime.now(timezone.utc))
    return df


def to_arrow(df: pd.DataFrame, ds: Dataset) -> pa.Table:
    return pa.Table.from_pandas(df, schema=ds.full_schema, preserve_index=False)


# --------------------------------------------------------------------------- #
# Delta writing
# --------------------------------------------------------------------------- #
def write_bronze(table: pa.Table, ds: Dataset, mode: str) -> None:
    ds.target_dir.mkdir(parents=True, exist_ok=True)
    if mode == "overwrite":
        write_deltalake(str(ds.target_dir), table, mode="overwrite", schema_mode="overwrite")
    else:
        write_deltalake(str(ds.target_dir), table, mode="append")
    log.info("[%s] %d row(s) written to %s (mode=%s)",
             ds.table_name, table.num_rows, ds.target_dir, mode)


def run(mode: str) -> dict[str, pd.DataFrame]:
    dataframes: dict[str, pd.DataFrame] = {}
    for ds in DATASETS:
        log.info("Processing %s/%s", ds.schema_name, ds.table_name)
        files = select_files(ds, mode)
        if mode == "incremental" and not files:
            log.info("[%s] nothing new to ingest", ds.table_name)
            continue
        df = cast_to_schema(read_dataset(ds, files), ds)
        dataframes[ds.table_name] = df
        write_bronze(to_arrow(df, ds), ds, mode)
    return dataframes


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze ingestion -> Delta Lake")
    parser.add_argument("--mode", choices=["incremental", "overwrite"], default="incremental")
    args = parser.parse_args()
    run(args.mode)


if __name__ == "__main__":
    main()
