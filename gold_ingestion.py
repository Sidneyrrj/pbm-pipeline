"""
GOLD layer (medallion architecture) for pbm-pipeline.

Consumes the validated silver tables and writes the analytical tables to
``medallion/gold_delta/<table>``:

  * most_profitable : revenue ranking per pharmacy (sum(price))
  * reversal_rate   : reversal rate ranking per pharmacy
  * drug_prices     : unit price of each drug per pharmacy over time

Business rules:
  * A reverted claim (``id`` present in ``reverts.claim_id``) is treated as if it
    never happened for REVENUE and VOLUME -> excluded from most_profitable
    and drug_prices.
  * For the REVERSAL RATE it counts: rate = reverted claims / total claims
    (reverted + not reverted) of the pharmacy. Value between 0 and 1.
  * Several reverts for the same claim are counted only once.
  * Metrics are computed per ``npi``. If an npi belongs to more than one chain,
    ``chain`` lists the chains separated by ", " (e.g. "doctor, saint"), without
    duplicating the npi's value across several rows.

Gold tables are aggregates: every run recomputes everything from silver and
overwrites the tables.

Usage:
    python gold_ingestion.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent
MEDALLION_DIR = PROJECT_ROOT / "medallion"
SILVER_DIR = MEDALLION_DIR / "silver_delta"
GOLD_DIR = MEDALLION_DIR / "gold_delta"

SILVER_PATHS = {
    "pharmacies": SILVER_DIR / "pharmacy" / "pharmacies",
    "claims": SILVER_DIR / "claim_event" / "claims",
    "reverts": SILVER_DIR / "revert_event" / "reverts",
}

GOLD_SCHEMAS = {
    "most_profitable": pa.schema([
        pa.field("npi", pa.string(), nullable=False),
        pa.field("chain", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
    ]),
    "reversal_rate": pa.schema([
        pa.field("npi", pa.string(), nullable=False),
        pa.field("chain", pa.string(), nullable=False),
        pa.field("rate", pa.float64(), nullable=False),
    ]),
    "drug_prices": pa.schema([
        pa.field("npi", pa.string(), nullable=False),
        pa.field("chain", pa.string(), nullable=False),
        pa.field("ndc", pa.string(), nullable=False),
        pa.field("price_unit", pa.float64(), nullable=False),
        pa.field("timestamp", pa.timestamp("us"), nullable=False),
    ]),
}

CHAIN_SEPARATOR = ", "

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("gold")


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
def read_silver(name: str, columns: list[str]) -> pd.DataFrame:
    path = SILVER_PATHS[name]
    if not DeltaTable.is_deltatable(str(path)):
        raise FileNotFoundError(f"Silver table not found: {path}. Run silver_ingestion.py first.")
    return DeltaTable(str(path)).to_pandas(columns=columns)


def pharmacy_chains(pharmacies: pd.DataFrame) -> pd.DataFrame:
    """One row per npi; several chains become "chain_a, chain_b"."""
    out = (pharmacies.dropna(subset=["npi", "chain"])
           .groupby("npi")["chain"]
           .agg(lambda s: CHAIN_SEPARATOR.join(sorted(set(s))))
           .reset_index())
    multi = out[out["chain"].str.contains(CHAIN_SEPARATOR, regex=False)]
    if len(multi):
        log.info("%d npi(s) belong to more than one chain", len(multi))
    return out


def flag_reverted(claims: pd.DataFrame, reverts: pd.DataFrame) -> pd.DataFrame:
    reverted_ids = set(reverts["claim_id"].dropna())
    claims = claims.copy()
    claims["is_reverted"] = claims["id"].isin(reverted_ids)

    orphan = len(reverted_ids - set(claims["id"]))
    if orphan:
        log.warning("%d revert(s) reference claims that are not in silver (ignored)", orphan)
    log.info("claims: %d total | %d reverted | %d effective",
             len(claims), claims["is_reverted"].sum(), (~claims["is_reverted"]).sum())
    return claims


# --------------------------------------------------------------------------- #
# Gold tables
# --------------------------------------------------------------------------- #
def build_most_profitable(claims: pd.DataFrame, chains: pd.DataFrame) -> pd.DataFrame:
    """Revenue = sum of price of the NON-reverted claims, per pharmacy.
    Pharmacies without revenue appear with value 0."""
    revenue = (claims.loc[~claims["is_reverted"]]
               .groupby("npi", as_index=False)["price"].sum()
               .rename(columns={"price": "value"}))
    out = chains.merge(revenue, on="npi", how="left")
    out["value"] = out["value"].fillna(0.0).round(2)
    return (out.sort_values(["value", "npi"], ascending=[False, True])
               [["npi", "chain", "value"]].reset_index(drop=True))


def build_reversal_rate(claims: pd.DataFrame, chains: pd.DataFrame) -> pd.DataFrame:
    """rate = reverted claims / total claims of the pharmacy.
    Pharmacies without any claim are left out (undefined rate)."""
    stats = (claims.groupby("npi")
             .agg(total_claims=("id", "size"), reversals=("is_reverted", "sum"))
             .reset_index())
    stats["rate"] = (stats["reversals"] / stats["total_claims"]).round(4)
    out = stats.merge(chains, on="npi", how="inner")
    return (out.sort_values(["rate", "npi"], ascending=[False, True])
               [["npi", "chain", "rate"]].reset_index(drop=True))


def build_drug_prices(claims: pd.DataFrame, chains: pd.DataFrame) -> pd.DataFrame:
    """Unit price (price / quantity) of each NON-reverted claim.
    quantity > 0 is already guaranteed by silver."""
    valid = claims.loc[~claims["is_reverted"]].copy()
    valid["price_unit"] = (valid["price"] / valid["quantity"]).round(4)
    out = valid.merge(chains, on="npi", how="inner")
    return (out.sort_values(["npi", "ndc", "timestamp"])
               [["npi", "chain", "ndc", "price_unit", "timestamp"]].reset_index(drop=True))


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #
def write_gold(name: str, df: pd.DataFrame) -> None:
    path = GOLD_DIR / name
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=GOLD_SCHEMAS[name], preserve_index=False)
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")
    log.info("[%s] %d row(s) -> %s", name, table.num_rows, path)


def run() -> dict[str, pd.DataFrame]:
    pharmacies = read_silver("pharmacies", ["npi", "chain"])
    claims = read_silver("claims", ["id", "npi", "ndc", "price", "quantity", "timestamp"])
    reverts = read_silver("reverts", ["id", "claim_id", "timestamp"])

    chains = pharmacy_chains(pharmacies)
    claims = flag_reverted(claims, reverts)

    tables = {
        "most_profitable": build_most_profitable(claims, chains),
        "reversal_rate": build_reversal_rate(claims, chains),
        "drug_prices": build_drug_prices(claims, chains),
    }
    for name, df in tables.items():
        write_gold(name, df)
    return tables


if __name__ == "__main__":
    run()
