import sys
import zarr
import dask.array as da
import dask
import numpy as np
import matplotlib.pyplot as plt
import healpy as hp
import calendar
from datetime import datetime, date, timedelta

from z3fdb import (
    SimpleStoreBuilder,
    AxisDefinition,
    Chunking,
    ExtractorType
)

def compute_djf_means(zarr_path, data_start_date, start_year, end_year):
    """Compute DJF seasonal means for multiple years from 6-hourly data"""
   
    # no dask
    #arr = da.from_zarr(zarr_path)
    
    results = []
    
    for year in range(start_year, end_year + 1):
        # DJF period
        djf_start = date(year - 1, 12, 1)
        _, last_day_feb = calendar.monthrange(year, 2)
        djf_end = date(year, 2, last_day_feb)
        
        # Calculate indices, based on every 6 hours (*4)
        start_idx = 4*(djf_start - data_start_date).days
        end_idx = 4*(djf_end - data_start_date).days + 4  # Include last day
        
        # Compute mean
        #subset = arr[start_idx:end_idx, 0]
        #djf_mean = subset.mean(axis=0).compute()
        #no dask
        subset = zarr_path[start_idx:end_idx, 0]
        djf_mean = subset.mean(axis=0)
        
        results.append(djf_mean)
        print(f"DJF {year}: {djf_start} to {djf_end}")
        
   
    stack = np.stack(results, axis=0)
    #all_mean = np.mean(stack, axis=0)
    #print(f"All means shape: {all_mean.shape}")
    file="djf_mean_"+str(start_year)+"-"+str(end_year)+"_standard.npy"
    np.save(file, stack)
    return stack

MODELS = ['IFS-NEMO'] # ['IFS-NEMO','IFS-FESOM','ICON']
# hist
# Historical period
# means it finishes 31/12/2014
HIST_YEARS = range(1990, 2015)
HIST_EXPERIMENT = 'hist'        # activity: baseline
RESOLUTION = 'standard'

if isinstance(HIST_YEARS, (int, np.integer)):
  year_str1 = str(HIST_YEARS)
else:
  year_str1 = '/'.join(str(y) for y in HIST_YEARS)

year_str1 = "year="+year_str1+","

# define param
param_str = "type=fc,levtype=sfc,param=167"

month_str = "month=1/2/3/4/5/6/7/8/9/10/11/12,"

mars_request_hist="class=d1,dataset=climate-dt,activity=baseline,experiment=" + HIST_EXPERIMENT + ",generation=2,realization=1,expver=0001,stream=clmn," + "model="+MODELS[0]+",resolution="+RESOLUTION+"," + year_str1 + month_str + param_str

date_str1 = "date=19900101/to/20151231/by/1,"
time_str  = "time=0000/to/1800/by/6,"

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
        # Dim 0: date and time merged — 2x24 = 48 entries, time varies fastest
        AxisDefinition(["date","time"], Chunking.SINGLE_VALUE),
        # Dim 1: step — 2 entries
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
        # Dim 0: date and time merged — 2x24 = 48 entries, time varies fastest
        AxisDefinition(["date","time"], Chunking.SINGLE_VALUE),
        # Dim 1: step — 2 entries
        AxisDefinition(["activity","experiment"], Chunking.SINGLE_VALUE)
    ],
    ExtractorType.GRIB,
)
store2 = builder2.build()
scen = zarr.open_array(store2, mode="r", zarr_format=3, use_consolidated=False)
print("shape scen:",scen.shape)

#sys.exit(1)

# Wrap in dask
start_date_hist = date(1990, 1, 1)
start_date_scen = date(2015, 1, 1)

#print("compute djf means hist in chunks")
hist_years = list(range(1991, 2014, 4))
chunks = []
#  djf_mean = compute_djf_means(hist, start_date_hist, years, years+3)
for years in hist_years:
  file="djf_mean_"+str(years)+"-"+str(years+3)+"_standard.npy"
  print(f"DJF : {years} to {years+3}")
  djf_mean = np.load(file)
  chunks.append(djf_mean)

stack_years = np.concatenate(chunks, axis=0)
djf_hist_mean = stack_years.mean(axis=0)

print("compute djf means scen in chunks")
scen_years = list(range(2016, 2047, 4))
chunks = []
#  djf_mean = compute_djf_means(scen, start_date_scen, years, years+3)
for years in scen_years:
  file="djf_mean_"+str(years)+"-"+str(years+3)+"_standard.npy"
  print(f"DJF : {years} to {years+3}")
  djf_mean = np.load(file)
  chunks.append(djf_mean)

# add the rest
#djf_mean = compute_djf_means(scen, start_date_scen, 2048, 2049)
print(f"DJF : {2048} to {2049}")
file="djf_mean_"+str(2048)+"-"+str(2049)+"_standard.npy"
djf_mean = np.load(file)
chunks.append(djf_mean)

stack_years = np.concatenate(chunks, axis=0)
djf_scen_mean = stack_years.mean(axis=0)

# Load back part stack (saved in compute_djf_means)
# djf_scen_mean = np.load('djf_mean_2008-2012_standard.npy')
# chunks.append(djf_scen_mean)

diffs = {}
for model in MODELS:
  arr = djf_scen_mean - djf_hist_mean
  print(arr.shape)
  diffs[model] = arr
  # Individual statistics
  print(f"Max: {arr.max()}")
  print(f"Min: {arr.min()}")
  print(f"Std: {arr.std()}")
  print(f"Mean: {arr.mean()}")

# Plot climate change signal for each model
vmin = -2.5
vmax = 2.5

fig = plt.figure(figsize=(18, 5))
for i, model in enumerate(MODELS):
    hp.mollview(
        diffs[model], nest=True, flip='geo',
        cmap='RdBu_r', min=vmin, max=vmax,
        title=f'{model}: \u0394T (DJF {SCEN_EXPERIMENT} {SCEN_YEARS[0]}-{SCEN_YEARS[-1]}'
              f' minus {HIST_EXPERIMENT} {HIST_YEARS[0]}-{HIST_YEARS[-1]})',
        unit='K', sub=(1, 1, i + 1),
    )
    plt.savefig(f'standard-djf.png', dpi=150, bbox_inches='tight')
    plt.close()
