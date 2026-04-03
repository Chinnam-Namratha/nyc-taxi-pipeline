# NYC Taxi — Data Engineering Assessment

This is my solution to the Firmable data engineering assessment. The pipeline takes NYC TLC Yellow Taxi data for 2023 (~38M rows across 12 monthly Parquet files), transforms it through a dbt model stack into analytics-ready mart tables, and orchestrates the whole thing with Airflow.

I used DuckDB instead of Snowflake — it reads Parquet files natively, runs entirely in-process, and needs zero infrastructure to set up locally. The SQL is standard ANSI so it would port to Snowflake without too much pain if needed.

---

## Repo layout

```
├── dbt/
│   ├── models/
│   │   ├── staging/         stg_yellow_trips, stg_taxi_zones
│   │   ├── intermediate/    int_trips_enriched (filtering + zone join)
│   │   └── marts/           fct_trips, dim_zones, agg_daily_revenue, agg_zone_performance
│   ├── tests/               two singular tests (total_amount >= fare, unique composite keys)
│   ├── macros/              generate_surrogate_key, test_column_in_range (generic test)
│   ├── seeds/               taxi_zone_lookup.csv (265 zones, already in the repo)
│   ├── dbt_project.yml
│   └── profiles.yml.example
├── dags/
│   └── nyc_taxi_daily_pipeline.py
├── queries/
│   ├── q1_top_zones_by_revenue.sql
│   ├── q2_hour_of_day_pattern.sql
│   └── q3_consecutive_gap_analysis.sql
├── spark/
│   └── process_historical.py
├── requirements.txt
└── .gitignore
```

---

## Getting started

### 1. Python environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Download the data

Grab the 12 monthly Parquet files from TLC and drop them in `data/`:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
...
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet
```

The zone lookup CSV is already in `dbt/seeds/` — you don't need to download that separately.

### 3. Configure dbt

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

The default config points DuckDB at `data/nyc_taxi.duckdb` and reads Parquet from `data/`. If you put the files somewhere else, set `PARQUET_DIR` before running:

```bash
export PARQUET_DIR=/path/to/your/data   # or $env:PARQUET_DIR on Windows
```

### 4. Run dbt

```bash
cd dbt

dbt seed    # loads taxi_zone_lookup into DuckDB
dbt run     # builds staging -> intermediate -> mart models
dbt test    # runs all data quality tests
```

### 5. Run the Airflow DAG locally (Windows)

The DAG file has a `__main__` block so you can run it directly without a scheduler:

```powershell
# activate venv first
.venv\Scripts\Activate.ps1

python dags/nyc_taxi_daily_pipeline.py 2023-01-01
```

This runs all six tasks in order: freshness check -> seed -> staging -> intermediate -> marts -> tests -> notify. Pass any 2023 date to process that month's data.

### 6. Run the SQL queries

```bash
duckdb data/nyc_taxi.duckdb < queries/q1_top_zones_by_revenue.sql
duckdb data/nyc_taxi.duckdb < queries/q2_hour_of_day_pattern.sql
duckdb data/nyc_taxi.duckdb < queries/q3_consecutive_gap_analysis.sql
```

### 7. PySpark historical processing (bonus)

```bash
spark-submit spark/process_historical.py \
    --input  "data/yellow_tripdata_*.parquet" \
    --output "output/agg_daily_revenue/"
```

---

## How the modelling is structured

I went with the classic three-layer dbt approach — staging views, intermediate views, mart tables.

**Staging** does nothing except rename columns to snake_case, cast types, and compute `trip_duration_minutes`. I deliberately kept all raw rows here rather than filtering early — this way data quality tests at the staging layer catch bad data in the full feed, not just the clean subset. Since it is a view there is no storage cost.

**Intermediate** is where the business logic lives. Filtering happens here (not in staging, not in marts) so there is exactly one definition of "valid trip" across the whole project. The filter rules are:
- `trip_distance > 0` — a trip has to actually go somewhere
- `fare_amount > 0` — negative or zero fares are either comps or upstream errors
- `passenger_count >= 1` — dispatched trips with no passengers are a data issue
- `trip_duration_minutes BETWEEN 1 AND 180` — sub-minute is a recording glitch, over 3 hours is an outlier that skews aggregations

The zone join also lives here. Both `fct_trips` and any future mart models get clean, enriched data without having to re-implement the join.

**Marts** are tables (not views) because they are expensive to compute — aggregating 38M rows every time a dashboard queries them would be painful. `fct_trips` is the full enriched fact table; the two `agg_` models pre-compute the most common analytical queries.

**Surrogate key**: TLC data has no single natural primary key, so I use MD5 over `(pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount)`. It is the standard dbt_utils pattern; I reimplemented the macro locally to avoid a package dependency.

---

## Brainstormer answers

### Revenue rank: monthly vs global?

I went with monthly rank (`PARTITION BY pickup_year, pickup_month`). A global rank across all of 2023 is basically static — Midtown Manhattan zones sit at the top every single month and the ranking tells you nothing useful.

Monthly ranking is more interesting operationally: you can see which zones are rising or falling over the year, spot seasonal patterns (tourist zones peaking in summer), and track whether a new development area is gaining traction. A global rank is still easy to derive downstream with a simple GROUP BY if anyone needs it, but the monthly version is the one with actual signal in it.

The full reasoning is in a comment block at the top of `agg_zone_performance.sql`.

### Blue/green approach for bad data in marts

If `run_dbt_tests` fails after the mart models have already run, we have potentially written bad data into tables that downstream BI tools are reading. The fix is to not overwrite the live schema until tests pass.

The approach: run mart models into a shadow schema (`marts_staging`) first. After tests pass, atomically swap `marts_staging` -> `marts` using a DuckDB `ALTER SCHEMA RENAME` inside a transaction — no data copied, just a metadata rename. If tests fail, drop `marts_staging`; the live `marts` schema is untouched.

In the Airflow DAG this maps to a `swap_schemas` task sitting between `run_dbt_tests` and `notify_success`, with a branch operator that skips the swap if tests failed. I have described the approach in the DAG docstring but did not implement the full DuckDB schema swap — that is noted as a trade-off below.

### Q3 gap analysis performance on 38M rows

The detailed breakdown with specific Snowflake features (clustering keys, Search Optimization Service, result caching, incremental materialisation) is in the comment block at the top of `q3_consecutive_gap_analysis.sql`. Short version: cluster the table by `(pickup_location_id, pickup_date)` so the LEAD() window function resolves each partition to a small slice of micro-partitions rather than scanning everything.

---

## Trade-offs I made

**Full-refresh tables**: all mart models rebuild from scratch on every run. For a real daily pipeline you would switch to incremental materialisation with a `unique_key` — rebuilding 38M rows daily is expensive. I left it as full-refresh because the dataset fits in memory and it is simpler to reason about.

**Blue/green schema swap**: described above and in the DAG docstring but not fully implemented. Getting the DuckDB transaction and Airflow branch operator wired together correctly would take more time than was available.

**Source freshness path**: hardcoded to check for Parquet files on disk. With a proper DB source you would run `dbt source freshness` directly.

**Email alerts**: configured in the DAG (`email_on_failure=True`) but only fire under a real Airflow scheduler with SMTP configured. Does not work in the local `__main__` runner on Windows.

---

## AI tools used

I used GitHub Copilot (Claude Sonnet) throughout this. Honestly it was less about generating code wholesale and more about having something that could hold the full context — all the dbt models, the DAG, the SQL queries — and help me think through edge cases and spot inconsistencies across layers.

Specific things it helped with: getting the dbt macro syntax right for DuckDB, the Airflow DAG backfill semantics with `data_interval_start`, and the window function patterns in the SQL queries.

I reviewed everything it produced and caught real issues along the way — the seed not being included in the DAG pipeline (which caused an actual failure when I ran it), hardcoded venv paths in the dbt commands, and the Airflow 3.x import crash on Windows that needed the `if __name__ != "__main__"` guard to fix.
