# Livestock Weather Risk Assessment — PySpark Starter

> **Use case:** Help a U.S. livestock business compare candidate states by *historical extreme-weather exposure* and plan for season-specific heat/cold/heavy-rain events. This is a **decision-support prototype**, not a real-world farm suitability model or forecast.
>
> **课程项目 / Final Project:** Based on the team's proposal using NOAA GHCN-Daily (2000–2025), cleaned with Spark, stored in Parquet, aggregated via Spark SQL/DataFrame operations, and summarized for livestock location planning.

## 1. 先看什么 / Where to start

- `notebooks/final_demo.ipynb` — **最终演示入口**。从合成样例开始跑通 ETL → Weather Analytics → Scoring → 排名与图表。
- `scripts/run_pipeline.py` — 不打开 Notebook 也可以一键运行。
- `src/livestock_weather/` — 三位组员分别开发/维护的核心模块。
- `docs/PROJECT_PLAN.md` — 任务边界、数据契约、分工、里程碑、风险。

## 2. 技术栈与架构

```text
NOAA by_year/*.csv.gz  +  ghcnd-stations.txt
                |
                v
[01 ETL: etl.py] station metadata / QC / missing / unit conversion
                |
        clean_daily.parquet
                |
                v
[02 Weather Analytics: analytics.py]
  station-year valid-day denominators -> state-year -> state summary/seasonality
                |
      state_annual.parquet / state_summary.parquet
                |
                v
[03 Risk scoring: scoring.py]
  normalize heat/cold/rain exposure -> weighted 0–100 relative score -> state ranking
                |
        state_ranking.parquet
                |
                v
[final_demo.ipynb] scenario: compare TX, IA, WI + figure + limitations
```

## 3. 环境配置

Prerequisites: **Python 3.10+** and a working **Java runtime** compatible with your PySpark version. Your course JupyterHub may already supply Spark; if so, use its environment rather than creating a conflicting Spark install.

```bash
# In the repository root:
python -m venv .venv
source .venv/bin/activate               # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

If running notebooks, open with JupyterLab/Jupyter Notebook or VS Code's notebook support. If your environment doesn't have Jupyter installed, add `pip install jupyterlab` (not required for the CLI).

### 3a. 最快验证：合成样例（不需要联网）

```bash
python scripts/run_pipeline.py --demo
pytest -q
```

This creates 2 synthetic years (2000, 2001), 3 states (TX/IA/WI), 2 stations per state, and writes:

```text
data/demo_raw/                # synthetic example shaped like NOAA; NOT genuine observations
  2000.csv, 2001.csv, ghcnd-stations.txt
data/demo_processed/
  clean_daily.parquet/
  state_annual.parquet/
  state_summary.parquet/
  state_seasonal.parquet/
  state_ranking.parquet/
reports/figures/demo_state_ranking.png
```

**Do not submit demo output as actual NOAA findings.** Demo raw and processed data live in separate `data/demo_*` directories and are gitignored.

### 3b. 使用真实 NOAA 数据（从一年开始）

```bash
# Warning: each full yearly file may be large.
# Synthetic files are stored separately under `data/demo_raw/`, so they will not contaminate `data/raw/`.
python scripts/download_noaa.py --years 2024
python scripts/run_pipeline.py --config config/demo.json
```

The `demo.json` coverage settings in the last command are lenient for pipeline experimentation, **not for interpreting scientific results**. A single year cannot satisfy the production `min_years_for_ranking=10` setting, so this is a pipeline smoke run only.

For the planned analysis, progressively download multiple years and run:

```bash
python scripts/download_noaa.py --years 2000 2001 2002 2003 2004
# Continue until the intended coverage period is present (2000–2025).
python scripts/run_pipeline.py --config config/settings.json
```

**Storage:** NOAA yearly CSV/GZIP files can be substantial. `download_noaa.py` does *not* auto-download the entire 26-year corpus; check your storage, network and Spark resources first. Reading `*.csv*` handles `.csv` and `.csv.gz`. Large-scale runs may be more practical on the course Spark/JupyterHub environment.

## 4. Pipeline interface / 三人交接契约

| Stage | Input | Output (Parquet) | Contract |
| --- | --- | --- | --- |
| ETL | NOAA records + ghcnd-stations.txt | `clean_daily.parquet` | `station_id, state, latitude, longitude, date, tmax_c, tmin_c, prcp_mm, year, month, season` |
| Analytics | clean_daily | `state_annual.parquet` | `state, year, heat_rate, cold_rate, rain_rate, heat_stations, cold_stations, rain_stations` |
| Analytics | state_annual | `state_summary.parquet` | `state, heat_rate, cold_rate, rain_rate, heat_years, cold_years, rain_years` |
| Analytics | clean_daily | `state_seasonal.parquet` | `state, season, heat_rate, cold_rate, rain_rate, *_station_seasons` |
| Risk scoring | state_summary + `config/settings.json` | `state_ranking.parquet` | `state, *_rate, *_score, overall_score, rank` |

Each `*_rate` is the fraction of **valid observed days** satisfying the criterion, averaged over qualifying stations and then over qualifying years. **It is not a count of all stations' events combined** (which would bias states with many stations). Empty data remain null, never treated as zero events.

## 5. Decisions and assumptions (important for final report)

- **Illustrative event thresholds:** `TMAX >= 35°C`, `TMIN <= -10°C`, `PRCP >= 50 mm/day`. These are not livestock-specific physiological thresholds; justify or revise them using external evidence before drawing a substantive conclusion.
- **Unit conversion:** raw TMAX/TMIN are tenths of °C; PRCP tenths of mm. Rows with non-empty `Q_FLAG` or -9999 are discarded.
- **Coverage controls:** A station-year needs at least 180 valid observations *per metric* (production); each state must have 10 valid years *per metric* for scoring. Sample config relaxes these for tiny synthetic data.
- **Relative score:** `0–100` **min-max scaled across states that pass coverage thresholds**. `overall = 0.40 heat + 0.35 cold + 0.25 rain`; these are default decision weights, not empirically estimated losses. Lower means lower historical exposure *under that chosen definition*.
- **Seasons:** DJF/MAM/JJA/SON labels group calendar-year observations; DJF currently pools Jan/Feb and Dec of the **same calendar year**. This is appropriate for broad seasonal profiles, not studies requiring consecutive winters spanning December to February.
- **Geospatial limitation:** Weather stations are not uniformly distributed. Equal-weight station averages and year averages do not yield an area-weighted geographic exposure estimate. Show station counts, explore coverage bias, or implement gridding if time permits.
- **Decision limitation:** Severe weather exposure is only one consideration for farm location. This prototype does **not** estimate animal injury/death, economic loss, livestock-specific suitability, business return or future weather. A state ranking alone cannot determine the best farm location.
- **Scalability:** Parquet partitioned by year at the daily level; Spark handles full records. Only small state-level summaries are collected for Matplotlib visualizations.
- **Repeatability:** NOAA updates historical records, so note the access date and data version in the final report.

## 6. How GitHub helps / GitHub 的作用

A **single private GitHub repository** is enough. GitHub is for version control, code reviews, and combining the three modules — **not** where Spark runs and **not** a data warehouse. Commit Python code, notebooks, sample documentation and config only; don't upload full NOAA raw/Parquet files.

Suggested branches: `feature/etl`, `feature/weather-analytics`, `feature/risk-scoring`; merge via PR after local tests. Avoid everyone editing `final_demo.ipynb` at the same time. Keep notebook outputs small to prevent oversized commits.

## 7. Data references

- NOAA GHCN-Daily by_year: https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/
- NOAA year-file field definitions: https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/readme-by_year.txt
- NOAA station metadata: https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
- NOAA README (element units, QC, methods): https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt

See `docs/PROJECT_PLAN.md` for task allocation and delivery checklist.
