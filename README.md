# Team: <Jules CINC> & <Luka RONDAL>

**DAG id:** `team_CR`  
**Git repo:** `https://github.com/lrondal/big-data-airflow` - **also on your Moodle slides** (title or architecture)  
**Spark module:** `include/team_CR_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

<Who needs the dashboard? What breaks if the pipeline fails?>

**Defense tip:** for each section below, be ready to say **what you built** and **why** (not only that it runs).

**Submit by June 9, 23:59:** push capstone to **your pair's Git repo**; upload **slides on Moodle** with the **same URL** visible on the slides (title slide). Public repo, or private with instructor read access.

---

## 2. Architecture

<!-- Diagram: incoming → raw/dt= → curated/dt= → reports -->

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=` | **Your** `team_CR_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `wait_for_csv` | `Wait for csv to exist in data/incoming` |
| `ingest_to_silver` | `Call DuckDB to convert csv -> Silver Parquet` |
| `validate_silver` | `Check that Silver isn't empty and that amount_eur isn't NULL everywhere` |
| `run_spark_kpis` | `-> Gold Parquet` |
| `publish_report` | `Write a json file as report` |

**Dependency graph:**

```
`wait_for_csv` → `ingest_to_silver` → `validate_silver` → `run_spark_kpis` → `publish_report`
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_CR_spark.py`

# | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Read raw Parquet for a given logical date and filter rows where `amount_eur > 0`, `tx_id`, `category` and `country` are not null |
| 2 | `transform_2` | Join with reference CSV targets, then derive computed columns: `transaction_hour`, `transaction_date`, `amount_category`, `is_card_payment`, `target_achievement_pct` |
| 3 | `transform_3` | Aggregate KPIs into two DataFrames: one grouped by `category`, one by `country`, both ordered by descending `total_revenue_eur` |

---s

## 4. Idempotence

<Re-run same `ds`: what gets overwritten under `raw/dt=`, `curated/dt=`, `dashboard_*.json`?>

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_<shortname> -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

<Which task fails? What appears in the Airflow UI?>

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | | |
| S Spark depth | | |
| O Orchestration | | |
| Q Data quality | | |
| P Custom | | |
| X SparkSubmit | | |

---

## 8. Demo script & backup

---

## 9. Production next steps


## 10. Collaborators
Please, follow commit keywords as explained [here](https://buzut.net/cours/versioning-avec-git/bien-nommer-ses-commits).

