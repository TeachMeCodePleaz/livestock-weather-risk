# Livestock Weather Risk Assessment and Location Comparison Using Apache Spark

**A scalable, data-driven decision-support prototype for comparing historical extreme-weather exposure across U.S. states.**

## Project overview

Extreme heat, severe cold, and heavy precipitation can create operational challenges for livestock businesses, including seasonal preparedness and infrastructure planning. A prospective operator comparing U.S. locations needs more than plots of long-term weather trends: it needs a consistent way to compare the *frequency, composition, and seasonality* of relevant weather hazards.

This project processes daily station observations from NOAA's **Global Historical Climatology Network–Daily (GHCN-Daily)** with **Apache Spark**. It converts raw records into comparable extreme-weather exposure metrics, builds a transparent state-level scoring framework, and presents candidate-location comparisons for an illustrative livestock-business scenario.

**Research question:** *Among candidate U.S. states, which have lower historical exposure to selected extreme-weather events, and which seasonal hazards merit the most attention?*

**Primary use case:** A livestock business considering multiple operating locations—for example, **Texas (TX), Iowa (IA), and Wisconsin (WI)**—can compare their historical heat, cold, and heavy-rainfall exposure under an explicit set of assumptions and priorities.

> **Scope of interpretation:** The output is a **historical weather-exposure comparison**, not an agricultural suitability certification, a livestock-loss prediction, a financial assessment, or a forecast of future climate conditions.

## Objectives and expected deliverables

1. **Scalable processing:** ingest NOAA yearly files, connect observations to U.S. station metadata, filter invalid records, standardize units, and write analysis-ready Parquet datasets.
2. **Weather analytics:** calculate extreme-event frequencies with valid-observation denominators, summarize annual and seasonal patterns, and retain coverage indicators.
3. **Relative exposure index:** normalize heat, cold, and precipitation metrics across eligible states; combine them using configurable weights; and rank states by the resulting score.
4. **Decision support:** compare selected candidate states, identify their dominant weather-exposure dimensions, and discuss implications for seasonal preparedness.
5. **Reproducibility and evaluation:** provide runnable PySpark modules, notebooks, synthetic integration data, tests, and documentation of assumptions and limitations.

The intended study period is **2000–2025**, subject to data availability, quality controls, and course computing resources. The repository currently provides a **working design and starter implementation**; published rankings based on the full historical dataset are **not yet included**.

## Technical approach

```text
NOAA GHCN-Daily yearly observations + ghcnd-stations.txt
                           |
                           v
  [1] Ingestion and preprocessing (PySpark)
      - U.S. station selection; missingness / quality-flag handling
      - TMAX / TMIN / PRCP unit conversion; station-day consolidation
                           |
                 clean_daily.parquet
                           |
                           v
  [2] Extreme-weather analytics (Spark DataFrame operations / SQL)
      - Station-year event rates with observed-day denominators
      - State-year summaries; state-level seasonal exposure
                           |
          state_annual.parquet | state_summary.parquet
                           |   state_seasonal.parquet
                           v
  [3] Relative risk scoring (PySpark)
      - Coverage eligibility; min-max normalization
      - Weighted index; candidate-state comparison and ranking
                           |
                state_ranking.parquet
                           |
                           v
  [4] Visualization and decision support
      - Notebook tables, comparison scenario, ranking plot
      - Interpretation, caveats, and next-step recommendations
```

The project intentionally separates **distributed processing** from **presentation**: large daily datasets remain in Spark/Parquet, while the plotting module collects only small, state-level aggregates.

## Data sources and scope

| Source | Purpose | Notes |
| --- | --- | --- |
| [NOAA GHCN-Daily: `by_year`](https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/) | Daily weather observations | Yearly compressed CSV files; target years 2000–2025 |
| [NOAA station metadata](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt) | Station-to-location mapping | Station identifier, latitude, longitude, and U.S. state code |
| [NOAA yearly-file documentation](https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/readme-by_year.txt) | Field definitions | CSV ordering and flag semantics |
| [NOAA dataset documentation](https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt) | Measurement definitions | Element units, missing-value conventions, and quality information |

### Weather variables

| Variable | Meaning | Original unit | Analysis unit |
| --- | --- | --- | --- |
| `TMAX` | Daily maximum temperature | Tenths of °C | °C (`tmax_c`) |
| `TMIN` | Daily minimum temperature | Tenths of °C | °C (`tmin_c`) |
| `PRCP` | Daily precipitation | Tenths of mm | mm (`prcp_mm`) |

The current ETL implementation filters invalid/missing sentinel values and records with populated `Q_FLAG`, joins U.S. stations, and pivots available elements to station-day records. Missing observations remain **null**, not zero. Duplicate station/date/element values are currently resolved using a maximum during aggregation and should be audited when processing the full dataset.

## Methodology

### 1. Extreme-weather events

The initial event definitions are configurable in `config/settings.json`:

| Exposure measure | Initial definition | Metric |
| --- | --- | --- |
| Extreme heat | `TMAX >= 35°C` | Fraction of valid `TMAX` observation days |
| Extreme cold | `TMIN <= -10°C` | Fraction of valid `TMIN` observation days |
| Heavy precipitation | `PRCP >= 50 mm/day` | Fraction of valid `PRCP` observation days |

These thresholds are **illustrative analytical definitions**, not validated physiological or operational limits for a particular livestock species. Threshold choice should be documented and tested for sensitivity before interpreting real-data results.

For metric \(m\), the station-year exposure rate is:

\[
 r_{s,y,m} = \frac{\text{valid observed days at station }s\text{ in year }y\text{ exceeding threshold }m}
 {\text{valid observed days for metric }m\text{ at station }s\text{ in year }y}.
\]

This avoids treating missing days as non-events or increasing a state's risk merely because it has more reporting stations. Qualified station-year rates are averaged within each state-year, then across qualifying years. Seasonal summaries aggregate qualified station-year-season rates.

**Coverage requirements in the production configuration:** at least **180 valid days per station-year per metric**, **30 valid days per station-season per metric**, and **10 qualifying state-years per metric** before a state is eligible for the overall ranking. The synthetic demo uses deliberately relaxed settings.

### 2. State-level exposure index

For eligible states, each weather-exposure rate is min-max normalized across the comparison cohort to a score between **0 and 100**. A larger component score represents greater exposure to the corresponding threshold-defined weather event.

The default composite score is:

\[
\text{ExposureScore}_i = 0.40 H_i + 0.35 C_i + 0.25 P_i,
\]

where \(H_i\), \(C_i\), and \(P_i\) are normalized heat, cold, and precipitation scores for state \(i\). **Lower is better only within this explicitly defined historical-exposure framework.** The weights are configurable business-preference assumptions, not calibrated estimates of livestock losses.

Because min-max scores depend on the set of eligible states, the numerical scores can change when the comparison cohort or coverage rules change. Ties and zero-variation metrics also require careful interpretation.

### 3. Candidate-state decision support

The example presentation focuses on **TX, IA, and WI** to demonstrate how a business might:

- compare overall relative exposure;
- inspect whether heat, cold, or rainfall drives the comparison;
- review seasonal patterns for operational planning;
- reconsider the ranking under alternative thresholds or weights.

A weather-exposure ranking is intended to inform **preliminary screening**, not to replace location-specific infrastructure, land, water, feed, veterinary, economic, or regulatory analysis.

## Repository organization

```text
livestock-weather-risk/
├── .github/workflows/tests.yml           # CI smoke tests on pushes and PRs
├── config/
│   ├── settings.json                      # Baseline coverage rules and thresholds
│   └── demo.json                          # Relaxed settings for synthetic examples
├── data/
│   ├── raw/                               # NOAA downloads (ignored by Git)
│   └── processed/                         # Spark output (ignored by Git)
├── docs/PROJECT_PLAN.md                   # Tasks, interfaces, evaluation checklist
├── notebooks/
│   ├── 01_data_processing.ipynb
│   ├── 02_weather_analysis.ipynb
│   ├── 03_risk_assessment.ipynb
│   └── final_demo.ipynb                   # Main demonstration notebook
├── reports/figures/                      # Locally generated figures
├── scripts/
│   ├── download_noaa.py                   # Optional yearly NOAA downloader
│   ├── make_demo_data.py                  # Synthetic station observations
│   └── run_pipeline.py                    # End-to-end command-line entry point
├── src/livestock_weather/
│   ├── etl.py                              # Data ingestion and cleaning
│   ├── analytics.py                        # Annual and seasonal exposure metrics
│   ├── scoring.py                          # Relative scores and state comparison
│   ├── pipeline.py                         # Stage orchestration and Parquet writes
│   ├── settings.py                         # Configuration validation
│   └── visuals.py                          # Aggregate-level ranking figure
├── tests/                                  # ETL, aggregation, configuration tests
├── requirements.txt
└── README.md
```

The three analytical stages have independent Python modules and corresponding notebooks to support parallel development. `final_demo.ipynb` is the integration and presentation entry point.

## Setup and execution

### Prerequisites

- Python **3.10+**
- A Java runtime compatible with the installed PySpark version (the CI workflow currently uses **Java 17** and **Python 3.11**)
- Apache Spark via PySpark; a course-managed Spark/Jupyter environment may be used instead of a local installation

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate                  # macOS / Linux
python -m pip install -r requirements.txt
```

The dependency list includes PySpark, Pandas, Matplotlib, notebook-format validation, and pytest. To launch a standalone notebook environment, install JupyterLab separately if necessary (`python -m pip install jupyterlab`), or use VS Code's Notebook support.

### Quick start: synthetic integration demo

```bash
python scripts/run_pipeline.py --demo
pytest -q
```

The demo creates **synthetic**, NOAA-shaped observations for TX, IA, and WI in 2000–2001, then executes the complete ETL → analytics → scoring pipeline. It writes local Parquet results under `data/demo_processed/` and a ranking image to `reports/figures/demo_state_ranking.png` when eligible data are present.

**Synthetic outputs are software demonstrations, not observed weather results, and must never be reported as empirical NOAA findings.**

Open `notebooks/final_demo.ipynb` for an integrated narrative; the individual notebooks can also be run in order (01 → 02 → 03). The main demo notebook creates its own synthetic inputs.

### Real NOAA data: incremental processing

Begin with a single year to test download, parsing, station joins, and local resource requirements:

```bash
python scripts/download_noaa.py --years 2024
python scripts/run_pipeline.py --config config/demo.json
```

This uses the **relaxed demo coverage settings on real data** for a *pipeline smoke test only*. A one-year run does not meet the production requirement of at least 10 qualifying years per metric, and its ranking should not be used for substantive conclusions.

After inspecting data quality and capacity, download additional years; for example:

```bash
python scripts/download_noaa.py --years 2000 2001 2002 2003 2004
# Continue until the planned analysis period has been acquired.
python scripts/run_pipeline.py --config config/settings.json
```

The downloader retrieves selected NOAA year files and station metadata into `data/raw/`. The full multi-year dataset may be large; choose the local machine or course computing environment accordingly. Raw downloads, generated Parquet data, local figures, and virtual environments are excluded from version control.

### Generated datasets and interface contracts

| Artifact | Key columns | Downstream consumer |
| --- | --- | --- |
| `clean_daily.parquet` | `station_id, state, latitude, longitude, date, tmax_c, tmin_c, prcp_mm, year, month, season` | Weather analytics |
| `state_annual.parquet` | `state, year, heat_rate, cold_rate, rain_rate, heat_stations, cold_stations, rain_stations` | Annual summary / trends |
| `state_summary.parquet` | `state, heat_rate, cold_rate, rain_rate, heat_years, cold_years, rain_years` | Risk scoring |
| `state_seasonal.parquet` | `state, season, heat_rate, cold_rate, rain_rate` and per-metric station-season counts | Seasonal interpretation |
| `state_ranking.parquet` | `state`, exposure rates, component scores, `overall_score`, `rank` | Candidate comparison / figure |

Spark writes Parquet **directories containing part files**, rather than one ordinary `.parquet` file. The daily table is partitioned by year.

## Testing and validation

The project includes automated tests covering configuration validity, missing-data denominators, unit conversion, quality-flag filtering, and basic scoring behavior. The workflow in `.github/workflows/tests.yml` runs `pytest -q` on GitHub-hosted Linux runners for pushes and pull requests.

**CI status is evidence of whether these checks pass; it does not demonstrate full-data scientific validity, production readiness, or large-scale performance.** Full evaluation should additionally address:

- input row counts, rejection rates, station coverage, and missingness by year/state/metric;
- extreme-event threshold choices and sensitivity of rankings to alternative weights;
- differences in station availability across geographic regions and years;
- reproducibility of aggregation across the complete intended period;
- Spark execution time, data volume, and the benefits of Parquet storage compared with raw CSV where feasible;
- a clear separation between observed historical exposure and assumed business priorities.

## Assumptions and limitations

1. **Uneven spatial sampling.** Equal-weight station averages are not population-weighted, area-weighted, or livestock-farm-weighted estimates. Sparse and geographically clustered station networks may distort state comparisons.
2. **Temporal coverage.** Station availability changes by year. Requiring a minimum number of valid observations and years reduces, but does not eliminate, coverage bias.
3. **Threshold dependence.** Different temperature/precipitation thresholds and weights may produce different rankings. The present values are design assumptions requiring justification.
4. **Season definition.** `DJF`, `MAM`, `JJA`, and `SON` use calendar-year month groupings; December is not shifted to the following winter year.
5. **Data quality and duplicate handling.** NOAA quality flags are filtered, but additional inspection of duplicate records and special observation flags may be necessary for research-grade analysis.
6. **Comparative, not absolute, scores.** Min-max normalization depends on the eligible states. The composite index is not an independently measured probability of loss.
7. **Limited business variables.** The dataset contains weather measurements, not farm outcomes, animal species, buildings, land costs, feed prices, or profitability. Results support preliminary screening only.
8. **Historical observations, not forecasts.** The model does not predict future extreme weather or quantify projected climate-change impacts.

## Development status and planned extensions

**Implemented in the starter:** modular PySpark ETL, observed-day weather-event rates, annual and seasonal state aggregations, configurable relative scores, Parquet output, a synthetic demo, notebooks, tests, and a small ranking visualization.

**Planned for the final project:** run and evaluate the complete 2000–2025 NOAA sample; verify station coverage and aggregation assumptions; document empirically observed patterns; compare candidate states using real data; add sensitivity analyses and performance measurements; prepare the final interpretation and course deliverables.

The implementation and findings should be updated as the team validates the NOAA pipeline and confirms any additional final-project requirements with the instructor.

## Data attribution

Weather records and station metadata originate from the **NOAA National Centers for Environmental Information (NCEI)**, GHCN-Daily dataset. Source documentation is linked above. Record the actual retrieval date and data version used in the final report, as historical observations may be revised.
