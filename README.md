# DestinE Climate Change Analysis

Analyse climate change signals from **DestinE Climate DT Generation 2** simulations.
Downloads monthly data (`clmn` stream) via Earthkit and [Polytope](https://github.com/ecmwf/polytope-client), computes 30-year mean differences between historical and SSP3-7.0 scenario experiments, and plots the results on the HEALPix grid.

Supports the three Climate DT models **IFS-NEMO**, **IFS-FESOM**, and **ICON**.

## Quick start

### 1. Clone this repository

```bash
git clone https://github.com/trackow/polytope-climatedt-analysis.git
cd polytope-climate-analysis
```

### 2. Set up the Python environment

**Option A: Using conda**

```bash
conda create -n destine-analysis python=3.13
conda activate destine-analysis
pip install earthkit-data healpy matplotlib numpy xarray pandas netcdf4 polytope-client ipykernel conflator lxml pydantic
```

**Option B: Using venv (no conda required)**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install earthkit-data healpy matplotlib numpy xarray pandas netcdf4 polytope-client ipykernel conflator lxml pydantic
```

If you are running on the **DESP**, the packages may already be available. Just make sure your kernel has `earthkit-data` and `healpy` installed.

### 3. Authenticate (once per session)

Open and run **`01_key_destine_once.ipynb`**. This will:

1. Clone the [polytope-examples](https://github.com/destination-earth-digital-twins/polytope-examples) repository (once)
2. Run the DestinE authentication script. You will be prompted for your **DESP credentials**
3. Store the API key in `~/.polytopeapirc`

You only need to do this **once**. All subsequent notebooks will pick up the key automatically.

### 4. Run the climate change analysis

Open and run **`02_climate_change_destine.ipynb`**. This notebook:

1. Downloads monthly 2m temperature data for the configured models and time periods
2. Computes the climate change signal (scenario mean minus historical mean)
3. Plots the result on the HEALPix grid, as a Mollweide map
4. Prints global-mean temperature change per model

The Configuration is in the beginning: you can change models, variables, time periods, and experiments there.

## Files

| File | Description |
|------|-------------|
| `01_key_destine_once.ipynb` | One-time authentication — stores your API key in order to access Climate DT data |
| `02_climate_change_destine.ipynb` | Climate change analysis notebook |
| `destine_climate_helpers.py` | Helper module (polytope request handling, caching, data retrieval, chunking over years) |

## Configuration options

All options are in the configuration cell of `02_climate_change_destine.ipynb`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PARAM` | `'avg_2t'` | Variable to analyse (e.g. `'avg_2t'`, `'235043'` for precip) |
| `MODELS` | `['IFS-NEMO', 'IFS-FESOM']` | Models to include (add `'ICON'` when available) |
| `RESOLUTION` | `'standard'` | Grid resolution (`'standard'` = H128, `'high'` = H1024) |
| `HIST_YEARS` | `range(1990, 2015)` | Historical period |
| `HIST_EXPERIMENT` | `'hist'` | Historical experiment name |
| `SCEN_YEARS` | `range(2015, 2050)` | Scenario period |
| `SCEN_EXPERIMENT` | `'SSP3-7.0'` | Scenario experiment name |
| `STORE_DATA` | `True` | Cache downloaded data as per-year NetCDF files |
| `DATA_DIR` | `'./data'` | Directory for cached data |

## Data caching

When `STORE_DATA = True`, downloaded data are saved as individual NetCDF files per year:

```
data/
├── IFS-NEMO/
│   ├── hist/clmn/standard/
│   │   ├── avg_2t_1990.nc
│   │   ├── avg_2t_1991.nc
│   │   └── ...
│   └── SSP3-7.0/clmn/standard/
│       ├── avg_2t_2015.nc
│       └── ...
├── IFS-FESOM/
│   └── ...
└── ICON/
    └── ...
```

Re-running the notebook skips years that are already cached. You do not need to think about this step.

## Requirements

- Python ≥ 3.10
- `earthkit-data`
- `polytope-client`
- `xarray`
- `healpy`
- `matplotlib`
- `numpy`
- `pandas`
- `netcdf4`
- A valid [DESP account](https://platform.destine.eu/) for Climate DT data, with upgraded access