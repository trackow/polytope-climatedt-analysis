import sys
import zarr
import dask.array as da
import dask
import numpy as np
import matplotlib.pyplot as plt
import healpy as hp

from destine_portfolio import (PORTFOLIO_GEN2_CLMN, PORTFOLIO_GEN2_CLTE, PORTFOLIO_GEN2_STORYLINE)

from datetime import date, timedelta

from z3fdb import (
    SimpleStoreBuilder,
    AxisDefinition,
    Chunking,
    ExtractorType
)

MODELS = ['IFS-FESOM'] # ['IFS-NEMO','IFS-FESOM','ICON']

# ── Choose a levtype (uncomment one) ──────────────────────────────
#LEVTYPE = "sfc"                    # 34 vars — surface (14 instant + 20 hourly mean)
# LEVTYPE = "pl"                   #  9 vars — pressure levels (19 levels)
# LEVTYPE = "hl"                   #  2 vars — height levels (100 m, IFS-only)
# LEVTYPE = "sol"                  #  2 vars — soil / snow
LEVTYPE = "o2d"                  # 12 vars — 2-D ocean & sea ice (daily)
# LEVTYPE = "o3d"                  #  5 vars — 3-D ocean (daily, up to 75 levels)

portfolio = PORTFOLIO_GEN2_CLTE
lt = portfolio[LEVTYPE]

keys=list(lt["variables"])
print(keys)

# hist
# Historical period

HIST_YEARS = range(1990, 2015)
HIST_EXPERIMENT = 'hist'        # activity: baseline
RESOLUTION = 'standard'

if isinstance(HIST_YEARS, (int, np.integer)):
  year_str1 = str(HIST_YEARS)
else:
  year_str1 = '/'.join(str(y) for y in HIST_YEARS)

year_str1 = "year="+year_str1+","

# define param
params = '/'.join(keys)
param_str = "type=fc,levtype="+LEVTYPE+",param="+params

month_str = "month=1/2/3/4/5/6/7/8/9/10/11/12,"

mars_request_hist="class=d1,dataset=climate-dt,activity=baseline,experiment=" + HIST_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clmn," + "model="+MODELS[0]+",resolution="+RESOLUTION+"," + year_str1 + month_str + param_str

date_str1 = "date=19900101/to/20151231/by/1,"
time_str  = "time=0000,"

mars_request_hourly_hist="class=d1,dataset=climate-dt,activity=baseline,experiment=" + HIST_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clte," + "model="+MODELS[0]+",resolution="+RESOLUTION+"," + date_str1 + time_str + param_str

# Future period
SCEN_YEARS = range(2015, 2050)
SCEN_EXPERIMENT = 'SSP3-7.0'   # activity: projections

if isinstance(SCEN_YEARS, (int, np.integer)):
  year_str2 = str(SCEN_YEARS)
else:
  year_str2 = '/'.join(str(y) for y in SCEN_YEARS)

year_str2 = "year="+year_str2+","

mars_request_scen="class=d1,dataset=climate-dt,activity=projections,experiment=" + SCEN_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clmn," + "model="+MODELS[0]+",resolution="+RESOLUTION+"," + year_str2 + month_str + param_str

date_str2 = "date=20150101/to/20501231/by/1,"
mars_request_hourly_scen="class=d1,dataset=climate-dt,activity=projections,experiment=" + SCEN_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clte," + "model="+MODELS[0]+",resolution="+RESOLUTION+"," + date_str2 + time_str + param_str

# NOTE: config.yaml contains the endpoint that needs to be reachable from the location
builder1 = SimpleStoreBuilder("./config.yaml")
builder1.add_part(
    mars_request_hourly_hist,
    [
        # Dim 0: date and time merged — no_datesx24 = entries, time varies fastest
        AxisDefinition(["date","time"], Chunking.SINGLE_VALUE),
        # Dim 1:
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        # Dim 2:
        AxisDefinition(["activity","experiment"], Chunking.NONE)
    ],
    ExtractorType.GRIB,
)

store1 = builder1.build()
hist = zarr.open_array(store1, mode="r", zarr_format=3, use_consolidated=False)
print("shape hist:",hist.shape)
print("hist info:",hist.info)

builder2 = SimpleStoreBuilder("./config.yaml")
builder2.add_part(
    mars_request_hourly_scen,
    [
        # Dim 0: date and time merged — no_datesx24 = entries, time varies fastest
        AxisDefinition(["date","time"], Chunking.SINGLE_VALUE),
        # Dim 1:
        AxisDefinition(["param"], Chunking.SINGLE_VALUE),
        # Dim 2:
        AxisDefinition(["activity","experiment"], Chunking.SINGLE_VALUE)
    ],
    ExtractorType.GRIB,
)
store2 = builder2.build()
scen = zarr.open_array(store2, mode="r", zarr_format=3, use_consolidated=False)
print("shape scen:",scen.shape)

# Example ocean
#ocefield = ds["avg_zos"].sel(model="IFS-FESOM", time="2014-01-01")
par = list(keys).index('avg_zos')

start_date_hist = date(1990, 1, 1)
start_date_scen = date(2015, 1, 1)

# for hist
target_date = date(2014, 1, 1)

days = (target_date - start_date_hist).days
position = days
print("position in array (time, par):" , position, par)

ocefield = hist[position,par,0]
print("shape field:",ocefield.shape)

hp.mollview(ocefield, title="IFS-FESOM — Sea Surface Height — 2014-01-01",
            unit="m", max=2.0, min=-2.0, cmap="RdYlBu_r", nest=True, flip='geo')
plt.savefig(f'ssh-fesom-standard.png', dpi=150, bbox_inches='tight')
plt.close()

