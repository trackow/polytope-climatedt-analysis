import zarr
import dask.array as da
import numpy as np
import matplotlib.pyplot as plt
import healpy as hp


from z3fdb import (
    SimpleStoreBuilder,
    AxisDefinition,
    Chunking,
    ExtractorType
)

MODELS = ['IFS-NEMO'] # ['IFS-NEMO','IFS-FESOM','ICON']
# hist
# Historical period
HIST_YEARS = range(1990, 2015)
HIST_EXPERIMENT = 'hist'        # activity: baseline

if isinstance(HIST_YEARS, (int, np.integer)):
  year_str1 = str(HIST_YEARS)
else:
  year_str1 = '/'.join(str(y) for y in HIST_YEARS)

year_str1 = "year="+year_str1+","

# define param
param_str = "type=fc,levtype=sfc,param=228004"

month_str = "month=1/2/3/4/5/6/7/8/9/10/11/12,"

mars_request_hist="class=d1,dataset=climate-dt,activity=baseline,experiment=" + HIST_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clmn," + "model="+MODELS[0]+",resolution=standard," + year_str1 + month_str + param_str

# Future period
SCEN_YEARS = range(2015, 2050)
SCEN_EXPERIMENT = 'SSP3-7.0'   # activity: projections

if isinstance(SCEN_YEARS, (int, np.integer)):
  year_str2 = str(SCEN_YEARS)
else:
  year_str2 = '/'.join(str(y) for y in SCEN_YEARS)

year_str2 = "year="+year_str2+","

mars_request_scen="class=d1,dataset=climate-dt,activity=projections,experiment=" + SCEN_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clmn," + "model="+MODELS[0]+",resolution=standard," + year_str2 + month_str + param_str

# NOTE: config.yaml contains the endpoint that needs to be reachable from the location
builder1 = SimpleStoreBuilder("./config.yaml")
builder1.add_part(
    mars_request_hist,
    [
        # Dim 0: date and time merged — 2x24 = 48 entries, time varies fastest
        AxisDefinition(["year","month"], Chunking.SINGLE_VALUE),
        # Dim 1: step — 2 entries
        AxisDefinition(["activity","experiment"], Chunking.NONE)
    ],
    ExtractorType.GRIB,
)

store1 = builder1.build()
hist = zarr.open_array(store1, mode="r", zarr_format=3, use_consolidated=False)
print("shape hist:",hist.shape)

builder2 = SimpleStoreBuilder("./config.yaml")
builder2.add_part(
    mars_request_scen,
    [
        # Dim 0: date and time merged — 2x24 = 48 entries, time varies fastest
        AxisDefinition(["year","month"], Chunking.SINGLE_VALUE),
        # Dim 1: step — 2 entries
        AxisDefinition(["activity","experiment"], Chunking.SINGLE_VALUE)
    ],
    ExtractorType.GRIB,
)
store2 = builder2.build()
scen = zarr.open_array(store2, mode="r", zarr_format=3, use_consolidated=False)
print("shape scen:",scen.shape)

# Wrap in dask
d_hist = da.from_zarr(hist)
hist_mean = d_hist.mean(axis=0).compute()
d_scen = da.from_zarr(scen)
scen_mean = d_scen.mean(axis=0).compute()

print("shape:",hist_mean.shape)

# Compute mean over time (assuming axis 0 is time)
diffs = {}
for model in MODELS:
  diffs[model] = scen_mean - hist_mean

# Plot climate change signal for each model
vmin = -2.5
vmax = 2.5

fig = plt.figure(figsize=(18, 5))
for i, model in enumerate(MODELS):
    hp.mollview(
        diffs[model][0], nest=True, flip='geo',
        cmap='RdBu_r', min=vmin, max=vmax,
        title=f'{model}: MONTHLY \u0394T ({SCEN_EXPERIMENT} {SCEN_YEARS[0]}-{SCEN_YEARS[-1]}'
              f' minus {HIST_EXPERIMENT} {HIST_YEARS[0]}-{HIST_YEARS[-1]})',
        unit='K', sub=(1, 1, i + 1),
    )
    plt.savefig(f'model.png', dpi=150, bbox_inches='tight')
    plt.close()
