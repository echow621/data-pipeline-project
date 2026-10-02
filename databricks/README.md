# Databricks Version

PySpark/Delta port of the local pandas project (see `../README.md` for the
overall architecture -- it's the same Bronze/Silver/Gold design, just running
on Databricks with Delta tables and `MERGE INTO` instead of CSV files.

## Important: workspace tier

**Databricks Community Edition (the free legacy tier) does not support the
Jobs API or Asset Bundles.** If you're using Community Edition, you can still
run `00_setup.py` -> `01_bronze_layer.py` -> `02_silver_layer.py` ->
`03_gold_layer.py` manually, one at a time, as notebooks -- everything except
the `databricks.yml` deployment/scheduling part will work.

For the full experience including automated deployment (`databricks bundle
deploy`) and the scheduled Job, you need either:
- **Databricks Free Edition** (the newer free tier that replaced Community
  Edition -- check current Databricks documentation, as offerings change), or
- A trial/paid workspace (AWS, Azure, or GCP)

## Manual run (works on any tier)

1. Import all five notebooks from `notebooks/` into your workspace, **including
   `_shared_utils.py`** -- the other three notebooks include it via `%run
   ./_shared_utils`, so it must sit in the same workspace folder as them
2. Run `00_setup.py` once (creates `orders_source`, `logistics_source`,
   `cdc_watermarks`)
3. Run `01_bronze_layer.py`, then `02_silver_layer.py`, then `03_gold_layer.py`
4. To see the CDC/incremental load in action: re-run `00_setup.py` with the
   `simulate_changes` widget set to `true`, then re-run the three layer
   notebooks -- only the new/changed rows should be processed

## Automated deployment (requires Jobs API / Asset Bundle support)

```bash
# Install the Databricks CLI (v0.220+) and authenticate
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# From the databricks/ directory:
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev bronze_silver_gold_pipeline
```

This creates a scheduled Job in your workspace with three chained tasks
(bronze -> silver -> gold), matching what `src/pipeline.py` does locally --
just orchestrated by Databricks Workflows instead of a Python script.

## CI/CD

See `.github/workflows/ci.yml` in the project root: on push to `main`, it
runs `databricks bundle deploy -t dev` using the `DATABRICKS_HOST` and
`DATABRICKS_TOKEN` repository secrets. This step is skipped automatically if
those secrets aren't configured (e.g. when only the local/pandas version is
being used).
