# Project Architecture & Team Work Plan

## Research question / Use case

**Which candidate U.S. states have relatively lower historical extreme-weather exposure for a livestock business, and what seasonal risks should the business plan around?**

The original proposal already contains livestock planning motivation but the instructor asked: **“ok/ what is the use case? just analyze patterns?”** This scaffold makes risk comparison and seasonal preparedness explicit final deliverables.

## Minimum viable project (MVP)

1. Load NOAA GHCN-Daily by_year data (2000–2025 target, start with a subset) and US station metadata.
2. Use PySpark for quality filtering, unit conversions, Parquet storage, aggregations.
3. Calculate state-level heat, cold and heavy precipitation event rates with valid-day denominators.
4. Implement a transparent, configurable weather-exposure score and eligibility rule.
5. Compare a hypothetical business case: TX vs IA vs WI, explain the main risk type and seasonal patterns.
6. Discuss sampling bias, missingness, threshold selection, scoring sensitivity, and business limitations.

## Proposed responsibilities (subject to team discussion)

| Person (example allocation only) | Owned module | Done criteria |
| --- | --- | --- |
| **Gaochang** | `etl.py`, `scripts/download_noaa.py`, data-quality/Parquet | Reads raw NOAA files + station metadata, applies flags/units correctly, emits daily Parquet, records counts/performance |
| **Ruihan** | `analytics.py`, `notebooks/02_weather_analysis.ipynb` | Valid-day frequency metrics, annual & seasonal Spark aggregation, coverage assessment, trend figures |
| **Zhiqi** | `scoring.py`, `visuals.py`, `notebooks/03_risk_assessment.ipynb` | Weighted/normalized risk index, state ranking, scenario comparison, sensitivity analysis and figures |
| **Everyone** | `final_demo.ipynb`, tests, final presentation | End-to-end reproducible run, interpretation, reporting, review |

Assignments are **illustrative**, not claims that the team has agreed.

## Module handoff details

### ETL → Analytics

Required daily schema: station_id (string), state (2-letter), date (date), year (int), month (int), season (DJF/MAM/JJA/SON), tmax_c (double nullable), tmin_c (double nullable), prcp_mm (double nullable).

Quality bar: filter non-US stations, flagged/bad/missing values, stable schema and README instructions. For full data, supply observations per metric and station-year coverage statistics.

### Analytics → Scoring

Required state summary: state (string), heat_rate/cold_rate/rain_rate (0–1 or null), heat_years/cold_years/rain_years (int). Rates use eligible station-year fractions, averaged across stations and years. **Do not** sum station counts or use the total count of records as an event denominator.

### Scoring → Final demo

Output state, normalized per-event scores, overall_score (0–100), rank (ascending score). State comparison with caveats. Score is relative to comparison cohort and selected weights, not absolute financial or livestock risk.

## Development sequence

- **Day 1:** Agree on use case, sample states, thresholds, station coverage, outputs; create one private GitHub repo.
- **Phase A:** Generate/inspect demo -> test end-to-end. Teams develop independent Python modules and notebooks.
- **Phase B:** Replace synthetic with 1–2 years of NOAA, inspect missingness/performance and run on course Spark.
- **Phase C:** Expand to planned 2000–2025 period, adjust coverage threshold as needed, validate metric semantics.
- **Phase D:** Review ranking sensitivity, generate visualizations and document limitations.
- **Phase E:** Freeze reproducible notebook, README, tests, performance comparison, slides/report if required by instructor.

## Technical and methodological checks

- Compare raw CSV vs Parquet size/query time on a representative year, if performance metrics expected.
- Show number of input observations, number retained after QC, number of eligible station-years per state.
- Review possible multi-day precipitation/trace-value flags and duplicate station-day elements for real NOAA data.
- Check low station density states/years; reconsider state aggregation or mark findings exploratory.
- Try alternative heat/cold/rain thresholds and weight sets; rank instability is a finding, not an error.
- Do not claim data contain livestock outcomes or economic information.
- The instructor's exact code/report/demo requirements should override this starter's proposed deliverables.

## GitHub checklist

```bash
git init
git add .
git commit -m "Initialize PySpark weather-risk project"
# After creating a private repository on GitHub:
git branch -M main
git remote add origin https://github.com/ORG_OR_USER/REPO.git
git push -u origin main
```

Then invite teammates and open feature branches. **These commands are instructions; no remote repo was created.**
