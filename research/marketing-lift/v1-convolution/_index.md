# `v1-convolution/` — superseded marketing-lift approach

Modeled marketing lift as a convolution of paid-acquisition impulses with a retention kernel. Three-stage notebook flow:

| Notebook | Purpose |
|---|---|
| `01_signal_extraction.ipynb` | Extracts the marketing-signal time-series from mobile DAU |
| `02_retention_fit.ipynb` | Fits the retention kernel against installs vs. DAU |
| `03_forecast_projection.ipynb` | Projects forward by convolving expected paid installs with the kernel |

## Why superseded

Produced a Dec-15 lift much larger than later validated against actuals. Replaced by `../v2-real-data/`, which uses the marketing-team CSV directly and stitches it as a hybrid with the empirical Fenix gap. Kept here as historical context; no longer the active model.

## Data

`data/` holds the retention-fit JSON and intermediate parquets the notebooks consume. Notebook 01 writes them; 02 and 03 read them.
