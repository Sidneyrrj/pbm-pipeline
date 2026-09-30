# pbm-pipeline

A small data pipeline for pharmacy benefit (PBM) data built on a **medallion architecture** (bronze → silver → gold) using Python, pandas and [Delta Lake](https://delta.io/) (via the `deltalake` library, no Spark required).

It ingests pharmacy, claim and revert files from `data/`, validates and cleans them, isolates bad records in a quarantine layer, and produces analytical tables (revenue ranking, reversal rate and unit drug prices).

```
data/  ──►  bronze_delta/  ──►  silver_delta/  ──►  gold_delta/
 raw         typed copy          validated &          business
 files       of the raw data     deduplicated         metrics
                                      │
                                      └──►  quarantine_delta/
                                            invalid records + reasons
```

---

## Project layout

```
pbm-pipeline/
├── data/                     # input files (provided)
│   ├── pharmacies/*.csv
│   ├── claims/*.json
│   └── reverts/*.json
├── medallion/                # output Delta tables (created by the scripts)
│   ├── bronze_delta/
│   ├── silver_delta/
│   ├── quarantine_delta/
│   └── gold_delta/
├── bronze_ingestion.py       # step 1: data/   -> bronze
├── silver_ingestion.py       # step 2: bronze  -> silver + quarantine
├── gold_ingestion.py         # step 3: silver  -> gold
└── requirements.txt
```

---

## Input data

The `data/` folder contains one sub-folder per dataset. The folder name is the table name.

| Folder | Format | Files provided | Records |
|---|---|---|---|
| `data/pharmacies` | CSV | 1 | 17 pharmacies |
| `data/claims` | JSON (array of objects) | 28 | 27,076 claims |
| `data/reverts` | JSON (array of objects) | 4 | 308 reverts |

Files starting with `._` (macOS AppleDouble metadata) are also present in these folders. They are not data and are ignored automatically.

### Schemas

**Pharmacy** (CSV)

| field | type | notes |
|---|---|---|
| `npi` | string | identifier of the pharmacy |
| `chain` | string | the chain the pharmacy belongs to |

**Claim event** (JSON). A claim is created when a pharmacy submits a prescription fill; each one represents real money and a real fill.

| field | type | notes |
|---|---|---|
| `id` | string | UUID identifying the claim |
| `npi` | string | pharmacy that filled the claim |
| `ndc` | string | drug identifier |
| `price` | float | total price charged (`unit_price` × `quantity`) |
| `quantity` | integer/float | amount of the drug filled |
| `timestamp` | datetime | when the claim was filled |

**Revert event** (JSON). A revert cancels a previous claim.

| field | type | notes |
|---|---|---|
| `id` | string | UUID identifying the revert |
| `claim_id` | string | the claim being invalidated |
| `timestamp` | datetime | when the revert happened |

---

## Requirements

- Python 3.10 or newer
- The packages in `requirements.txt`: `pandas`, `pyarrow`, `deltalake`

The pipeline was developed against pandas 3.0, pyarrow 25 and deltalake 1.6.

---

## Setup

From the project root, create a virtual environment and install the dependencies.

**Windows (PowerShell)**

```powershell
cd C:\path\to\pbm-pipeline
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**macOS / Linux**

```bash
cd /path/to/pbm-pipeline
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

The examples below use the Windows path `.\.venv\Scripts\python.exe`. On macOS/Linux, replace it with `.venv/bin/python`.

---

## Running the pipeline

Run the three steps in order. Every script can be run from any folder: paths are resolved from the script's location.

### Step 1 — Bronze

```powershell
.\.venv\Scripts\python.exe bronze_ingestion.py
```

Reads every file in `data/` and writes it, typed but otherwise unchanged, to:

| Source | Delta table |
|---|---|
| `data/pharmacies` | `medallion/bronze_delta/pharmacy/pharmacies` |
| `data/claims` | `medallion/bronze_delta/claim_event/claims` |
| `data/reverts` | `medallion/bronze_delta/revert_event/reverts` |

- `npi` and `ndc` are kept as strings so leading zeros are not lost (e.g. `0123456789`).
- `quantity` is stored as float, since the source mixes integers and decimals.
- Every row gets two lineage columns: `_source_file` (the file it came from) and `_ingested_at` (UTC).

### Step 2 — Silver and quarantine

```powershell
.\.venv\Scripts\python.exe silver_ingestion.py
```

Validates each bronze record. Valid records go to `medallion/silver_delta/<schema>/<table>`; invalid ones go to `medallion/quarantine_delta/<schema>/<table>`.

**Validations**

| Check | Rule | Reason code |
|---|---|---|
| Strings | not null, a string, not empty after trimming | `<col>:null`, `<col>:not_string`, `<col>:empty` |
| Floats | not null, numeric, finite | `<col>:null`, `<col>:not_float` |
| Timestamps | not null, valid date/time in `YYYY-MM-DD HH:MM:SS` (time is kept) | `<col>:null`, `<col>:invalid_timestamp` |
| Referential integrity | a claim's `npi` must exist in the silver `pharmacies` table | `npi:not_in_pharmacies` |
| Business rule | a claim's `quantity` must be greater than zero | `quantity:not_positive` |
| Duplicate key | see below | `id:duplicate`, `npi+chain:duplicate` |

**Keys and duplicates**

- `claims` and `reverts` are keyed by `id`. When the same `id` appears more than once, the **valid** record with the **most recent `timestamp`** is kept.
- `pharmacies` is keyed by `npi` + `chain`, because the same pharmacy can belong to more than one chain. For repeated pairs, the version ingested last is kept.

**Quarantine tables** keep the original bronze values and add:

- `_rejection_reasons`: every reason the record failed, separated by `;` (e.g. `quantity:null;npi:not_in_pharmacies`)
- `_quarantined_at`: when the record was quarantined

Silver tables add `_processed_at` to the bronze lineage columns.

### Step 3 — Gold

```powershell
.\.venv\Scripts\python.exe gold_ingestion.py
```

Builds the analytical tables in `medallion/gold_delta/<table>` from the silver layer. Gold tables are recomputed from scratch and overwritten on every run.

**How reverts are handled:** a reverted claim (its `id` appears in `reverts.claim_id`) is treated as if it **never happened** for revenue and volume, so it is excluded from `most_profitable` and `drug_prices`. It still counts in `reversal_rate`, because the reversal itself is the signal being measured. Several reverts for the same claim count once.

**`most_profitable`**: pharmacies ranked by revenue.

| field | type | notes |
|---|---|---|
| `npi` | string | pharmacy that filled the claim |
| `chain` | string | the chain the pharmacy belongs to |
| `value` | float | total revenue: `sum(price)` of non-reverted claims |

Pharmacies with no revenue appear with `value = 0`.

**`reversal_rate`**: pharmacies ranked by the share of their claims that were reverted.

| field | type | notes |
|---|---|---|
| `npi` | string | identifier of the pharmacy |
| `chain` | string | the chain the pharmacy belongs to |
| `rate` | float | reverted claims / total claims of the pharmacy (0 to 1; `0.0162` = 1.62%) |

Pharmacies without any claim are left out, since their rate is undefined.

**`drug_prices`**: unit price of each drug per pharmacy over time, one row per non-reverted claim.

| field | type | notes |
|---|---|---|
| `npi` | string | identifier of the pharmacy |
| `chain` | string | the chain the pharmacy belongs to |
| `ndc` | string | drug identifier |
| `price_unit` | float | price charged per unit (`price / quantity`) |
| `timestamp` | datetime | when the claim was filled |

If an `npi` belongs to more than one chain, the gold tables show the chains together in `chain` (e.g. `"doctor, saint"`) instead of repeating the pharmacy's numbers on several rows.

---

## Expected results with the provided data

After running the three steps on the files in `data/`:

| Table | Rows | Notes |
|---|---|---|
| bronze `pharmacies` | 17 | |
| bronze `claims` | 27,076 | 2 claims have no `quantity` |
| bronze `reverts` | 308 | |
| silver `pharmacies` | 17 | |
| silver `claims` | 22,988 | |
| quarantine `claims` | 4,088 | 4,085 `npi:not_in_pharmacies` (NPIs `2345678901`, `6789012345`, `0000000000`), 2 `quantity:null`, 1 `quantity:not_positive` |
| silver `reverts` | 305 | |
| quarantine `reverts` | 3 | `id:duplicate`: the same revert `id` appears twice with different timestamps |
| gold `most_profitable` | 17 | top: `3456789012` (saint), 19,490,326.90 |
| gold `reversal_rate` | 17 | top: `4444444444` (health), 0.0162 |
| gold `drug_prices` | 22,728 | 22,988 silver claims minus 260 reverted |

45 of the silver reverts point to claims that were quarantined. They are ignored in the gold metrics, and `gold_ingestion.py` logs a warning about them.

---

## Inspecting the tables

Any Delta table can be loaded into pandas:

```powershell
.\.venv\Scripts\python.exe -c "from deltalake import DeltaTable; print(DeltaTable('medallion/gold_delta/most_profitable').to_pandas().sort_values('value', ascending=False))"
```

Useful examples:

```python
from deltalake import DeltaTable

# Top 5 pharmacies by reversal rate
DeltaTable("medallion/gold_delta/reversal_rate").to_pandas().nlargest(5, "rate")

# Sample of unit prices
DeltaTable("medallion/gold_delta/drug_prices").to_pandas().sample(10)

# Why were claims quarantined?
q = DeltaTable("medallion/quarantine_delta/claim_event/claims").to_pandas()
q["_rejection_reasons"].value_counts()
```

Delta does not guarantee row order when reading, so sort by `value` or `rate` when you need the ranking.

---

## Adding new data (incremental runs)

`bronze_ingestion.py` and `silver_ingestion.py` run in **incremental** mode by default, so new files can be dropped into `data/` and the pipeline re-run:

```powershell
.\.venv\Scripts\python.exe bronze_ingestion.py
.\.venv\Scripts\python.exe silver_ingestion.py
.\.venv\Scripts\python.exe gold_ingestion.py
```

- **Bronze** only ingests files whose name is not yet in the table (checked through `_source_file`) and appends them. Running it again with no new files changes nothing.
- **Silver** only processes bronze rows ingested after the last run. Valid rows are upserted (`MERGE`) on the table key, and invalid rows are appended to quarantine. Claims are checked against the **complete** silver `pharmacies` table.
- **Gold** is always fully recomputed.

### Full reprocessing

Both ingestion scripts accept `--mode overwrite`, which re-reads everything and rewrites the tables:

```powershell
.\.venv\Scripts\python.exe bronze_ingestion.py --mode overwrite
.\.venv\Scripts\python.exe silver_ingestion.py --mode overwrite
.\.venv\Scripts\python.exe gold_ingestion.py
```

Use it when:

- **You reprocessed bronze with `--mode overwrite`.** Run silver with `--mode overwrite` too, otherwise quarantine receives repeated rows.
- **Quarantined records should be re-evaluated.** For example, claims quarantined as `npi:not_in_pharmacies` stay in quarantine even after the pharmacy arrives. An overwrite run re-checks them.
- **A file changed but kept its name.** Bronze identifies files by name, so incremental mode does not pick up the change.

Previous versions are not lost on overwrite: Delta keeps the table history (time travel) until a `vacuum` is run.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Bronze table not found ... Run bronze_ingestion.py first.` | Run the steps in order: bronze → silver → gold. |
| `Silver table not found ... Run silver_ingestion.py first.` | Same as above. |
| `ModuleNotFoundError: No module named 'deltalake'` | Use the virtual environment's Python (`.\.venv\Scripts\python.exe`) and install `requirements.txt`. |
| Many claims in quarantine as `npi:not_in_pharmacies` | Expected with the provided data: those NPIs are not in the pharmacies file. |
