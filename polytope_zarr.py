"""
Virtual Zarr store backed by Polytope.

Presents DestinE Climate DT data as an xarray Dataset where metadata is
available instantly and data chunks are fetched lazily on access (e.g.
when plotting or calling .values).

Usage:
    from polytope_zarr import PolytopeZarrStore
    store = PolytopeZarrStore(address=..., collection=..., base_request=...,
                              coords=..., variables=..., internal_dims=...)
    ds = store.open()
"""

import json
import logging
import numpy as np
import pandas as pd
from collections.abc import MutableMapping

# Try to import VLenUTF8 for proper string coordinates
try:
    from numcodecs import VLenUTF8
    _HAS_VLENUTF8 = True
except ImportError:
    _HAS_VLENUTF8 = False


class PolytopeZarrStore(MutableMapping):
    """A read-only zarr v2 store that fetches data from Polytope on demand."""

    def __init__(self, address, collection, base_request, coords, variables,
                 internal_dims=None, time_fields=None, batch_dim="time",
                 batch_size=12, batch_years=1):
        """
        Parameters
        ----------
        address : str or dict
            Polytope server URL, or {model_name: url} for per-model routing.
        collection : str
            Polytope collection name (e.g. "destination-earth").
        base_request : dict
            Fixed request fields (class, dataset, experiment, …).
            Do NOT include year/month/day/time/param/model — those are per-chunk.
        coords : dict
            {dim_name: array-like} — e.g. {"time": pd.date_range(...),
            "cell": range(196608), "model": ["ICON", "IFS-FESOM"]}.
        variables : dict
            {var_name: {"dims": ("model", "time", "cell")}}.
        internal_dims : list, optional
            Dims that form a single chunk (e.g. ["cell"]).
        time_fields : list, optional
            Request fields extracted from timestamps. Default ["year", "month"].
        batch_dim : str, optional
            Dimension to batch over in a single Polytope request.  Default "time".
            Set to None to disable batching (one request per chunk).
        batch_size : int, optional
            Maximum number of months per year to fetch in one request.
            Default 12 (= all months of each year).
        batch_years : int, optional
            Maximum number of years to combine in a single Polytope request
            (only applies when batch_dim="time").  Default 1 — batch only
            within the same calendar year.  Set to 5 or more for multi-year
            slices; note that higher values pre-fetch more data on first access.
        """
        self._address = address
        self._collection = collection
        self._base_request = dict(base_request)
        self._internal_dims = set(internal_dims or [])
        self._time_fields = time_fields or ["year", "month"]
        self._batch_dim = batch_dim
        self._batch_size = batch_size
        self._batch_years = batch_years
        self._cache = {}
        self._kv = {}

        # Normalise coordinates to numpy arrays
        self._coords = {}
        self._dim_sizes = {}
        for name, vals in coords.items():
            if name == "time":
                arr = np.array(pd.DatetimeIndex(vals), dtype="datetime64[ns]")
            elif hasattr(vals, '__len__') and not isinstance(vals, str):
                arr = np.array(list(vals))
            else:
                arr = np.array(vals)
            self._coords[name] = arr
            self._dim_sizes[name] = len(arr)

        self._variables = dict(variables)
        self._build_metadata()

    # ── Metadata synthesis ──────────────────────────────────────────────

    def _build_metadata(self):
        """Populate self._kv with zarr v2 metadata and coordinate chunks."""
        self._kv[".zgroup"] = json.dumps({"zarr_format": 2}).encode()
        self._kv[".zattrs"] = json.dumps({}).encode()

        meta = {"zarr_consolidated_format": 1, "metadata": {
            ".zgroup": {"zarr_format": 2}, ".zattrs": {}
        }}

        # Coordinate arrays
        for name, arr in self._coords.items():
            self._write_coord(name, arr, meta)

        # Data variables (metadata only — chunks are lazy)
        for var_name, spec in self._variables.items():
            self._write_var_meta(var_name, spec, meta)

        self._kv[".zmetadata"] = json.dumps(meta).encode()

    def _write_coord(self, name, arr, meta):
        """Write a coordinate array's metadata + data chunk into the store."""
        if arr.dtype.kind == "M":  # datetime
            data = arr.astype("int64")
            zarray = self._zarray(shape=(len(arr),), chunks=(len(arr),),
                                  dtype="|i8", fill_value=0)
            zattrs = {"_ARRAY_DIMENSIONS": [name],
                       "units": "nanoseconds since 1970-01-01",
                       "calendar": "proleptic_gregorian"}
        elif arr.dtype.kind == "U" or arr.dtype.kind == "O":  # strings
            zarray, data = self._encode_strings(arr)
            zattrs = {"_ARRAY_DIMENSIONS": [name]}
        else:  # numeric
            data = arr.astype("int32") if arr.dtype.kind == "i" else arr
            zarray = self._zarray(shape=(len(arr),), chunks=(len(arr),),
                                  dtype=data.dtype.str, fill_value=None)
            zattrs = {"_ARRAY_DIMENSIONS": [name]}

        key_prefix = name
        self._kv[f"{key_prefix}/.zarray"] = json.dumps(zarray).encode()
        self._kv[f"{key_prefix}/.zattrs"] = json.dumps(zattrs).encode()
        self._kv[f"{key_prefix}/0"] = (
            bytes(data) if isinstance(data, (bytes, bytearray)) else data.tobytes()
        )

        meta["metadata"][f"{key_prefix}/.zarray"] = zarray
        meta["metadata"][f"{key_prefix}/.zattrs"] = zattrs

    def _write_var_meta(self, var_name, spec, meta):
        """Write a data variable's zarr metadata (no data — chunks are lazy)."""
        dims = spec["dims"]
        shape = tuple(self._dim_sizes[d] for d in dims)
        chunks = tuple(
            self._dim_sizes[d] if d in self._internal_dims else 1
            for d in dims
        )
        zarray = self._zarray(shape=shape, chunks=chunks,
                              dtype="<f4", fill_value="NaN")
        zattrs = {"_ARRAY_DIMENSIONS": list(dims)}
        # Propagate extra attributes (long_name, units, etc.) from spec
        for attr_key in ("long_name", "units"):
            if attr_key in spec:
                zattrs[attr_key] = spec[attr_key]

        self._kv[f"{var_name}/.zarray"] = json.dumps(zarray).encode()
        self._kv[f"{var_name}/.zattrs"] = json.dumps(zattrs).encode()

        meta["metadata"][f"{var_name}/.zarray"] = zarray
        meta["metadata"][f"{var_name}/.zattrs"] = zattrs

    @staticmethod
    def _zarray(shape, chunks, dtype, fill_value, compressor=None):
        return {"zarr_format": 2, "shape": list(shape),
                "chunks": list(chunks), "dtype": dtype,
                "fill_value": fill_value, "order": "C",
                "compressor": compressor, "filters": None}

    def _encode_strings(self, arr):
        """Encode a string array for zarr v2, using VLenUTF8 if available."""
        vals = [str(v) for v in arr]
        if _HAS_VLENUTF8:
            codec = VLenUTF8()
            data_bytes = codec.encode(np.array(vals, dtype=object))
            zarray = self._zarray(
                shape=(len(vals),), chunks=(len(vals),), dtype="|O",
                fill_value="", compressor=None)
            zarray["filters"] = [codec.get_config()]
            return zarray, data_bytes
        # Fallback: fixed-length bytes
        maxlen = max(len(v) for v in vals)
        data = np.array(vals, dtype=f"|S{maxlen}")
        zarray = self._zarray(shape=(len(vals),), chunks=(len(vals),),
                              dtype=data.dtype.str, fill_value="")
        return zarray, data.tobytes()

    # ── Lazy chunk fetching ─────────────────────────────────────────────

    def _chunk_key_for_indices(self, indices):
        """Build a dotted chunk key from a list of integer indices."""
        return ".".join(str(i) for i in indices)

    def _fetch_chunk(self, var_name, chunk_key):
        """Fetch one or more chunks via a batched Polytope request.

        When batch_dim is set (default: "time"), this looks for other
        uncached indices along that dimension and fetches up to batch_size
        of them in a single Polytope call (e.g. month=1/2/3/.../12).
        Results are split and cached individually.
        """
        spec = self._variables[var_name]
        dims = spec["dims"]
        indices = [int(i) for i in chunk_key.split(".")]

        # Identify the batch dimension position
        batch_pos = None
        if self._batch_dim and self._batch_dim in dims:
            batch_pos = list(dims).index(self._batch_dim)
            if self._batch_dim in self._internal_dims:
                batch_pos = None  # can't batch over an internal dim

        # Collect batch indices: uncached neighbours along batch_dim
        if batch_pos is not None:
            batch_indices = []
            dim_size = self._dim_sizes[self._batch_dim]

            # For time batching, collect uncached months within a year range
            # controlled by batch_years.  Polytope treats multi-value year/month
            # as a Cartesian product; metadata-based splitting handles this.
            if self._batch_dim == "time":
                ref_year = pd.Timestamp(self._coords["time"][indices[batch_pos]]).year
                max_year = ref_year + self._batch_years - 1
                max_total = self._batch_size * self._batch_years
                for i in range(dim_size):
                    ts_year = pd.Timestamp(self._coords["time"][i]).year
                    if ts_year < ref_year or ts_year > max_year:
                        continue
                    trial = list(indices)
                    trial[batch_pos] = i
                    trial_key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                    if trial_key not in self._cache:
                        batch_indices.append(i)
                    if len(batch_indices) >= max_total:
                        break
            else:
                for i in range(dim_size):
                    trial = list(indices)
                    trial[batch_pos] = i
                    trial_key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                    if trial_key not in self._cache:
                        batch_indices.append(i)
                    if len(batch_indices) >= self._batch_size:
                        break

            # Ensure the originally requested index is included
            if indices[batch_pos] not in batch_indices:
                batch_indices.append(indices[batch_pos])
                batch_indices.sort()
        else:
            batch_indices = None

        # Build the Polytope request
        request = dict(self._base_request)
        request["param"] = var_name
        chunk_shape = []

        for dim_i, (dim, idx) in enumerate(zip(dims, indices)):
            size = self._dim_sizes[dim]
            if dim in self._internal_dims:
                chunk_shape.append(size)
                continue
            chunk_shape.append(1)

            if dim == "time" and dim_i == batch_pos and batch_indices is not None:
                # Batched time request
                timestamps = [pd.Timestamp(self._coords["time"][bi])
                              for bi in batch_indices]
                field_map_all = {}
                for ts in timestamps:
                    fm = {"year": str(ts.year), "month": str(ts.month),
                          "day": str(ts.day), "time": ts.strftime("%H:%M")}
                    for f in self._time_fields:
                        field_map_all.setdefault(f, []).append(fm[f])
                for f in self._time_fields:
                    request[f] = "/".join(dict.fromkeys(field_map_all[f]))
            elif dim == "time":
                ts = pd.Timestamp(self._coords["time"][idx])
                field_map = {"year": str(ts.year), "month": str(ts.month),
                             "day": str(ts.day),
                             "time": ts.strftime("%H:%M")}
                for f in self._time_fields:
                    request[f] = field_map[f]
            elif dim == "model":
                request["model"] = str(self._coords["model"][idx])
            elif dim == "level":
                request["levelist"] = str(int(self._coords["level"][idx]))
            else:
                coord_val = self._coords[dim][idx]
                request[dim] = str(coord_val)

        # Resolve address
        address = self._address
        if isinstance(address, dict):
            model = request.get("model", "")
            address = address.get(model, list(address.values())[0])

        n_cells = 1
        for s in chunk_shape:
            n_cells *= s

        n_batch = len(batch_indices) if batch_indices is not None else 1
        if n_batch > 1:
            if self._batch_dim == "time" and batch_indices is not None:
                yrs = set(pd.Timestamp(self._coords["time"][bi]).year
                          for bi in batch_indices)
                yr_info = f" across {len(yrs)} years" if len(yrs) > 1 else ""
            else:
                yr_info = ""
            print(f"  ⚡ batching {n_batch} {self._batch_dim} chunks{yr_info} for {var_name}")

        try:
            import earthkit.data
            data = earthkit.data.from_source(
                "polytope", self._collection, request,
                address=address, stream=False)

            if batch_indices is not None and n_batch > 1:
                # Split the multi-field response into individual chunks
                self._split_batch(var_name, dims, indices, batch_pos,
                                  batch_indices, chunk_shape, data)
            else:
                values = data.to_numpy().flatten().astype(np.float32)
                if values.size != n_cells:
                    raise ValueError(
                        f"Size mismatch: got {values.size}, expected {n_cells}")
                store_key = f"{var_name}/{chunk_key}"
                self._cache[store_key] = values.tobytes()

        except Exception as e:
            print(f"  ⚠ fetch {var_name} chunk {chunk_key}: {e}")
            # Fill all batch indices with NaN on failure
            if batch_indices is not None:
                for bi in batch_indices:
                    trial = list(indices)
                    trial[batch_pos] = bi
                    key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                    if key not in self._cache:
                        self._cache[key] = np.full(
                            n_cells, np.nan, dtype=np.float32).tobytes()
            else:
                self._cache[f"{var_name}/{chunk_key}"] = np.full(
                    n_cells, np.nan, dtype=np.float32).tobytes()

        return self._cache.get(f"{var_name}/{chunk_key}",
                               np.full(n_cells, np.nan, dtype=np.float32).tobytes())

    def _split_batch(self, var_name, dims, indices, batch_pos,
                     batch_indices, chunk_shape, data):
        """Split a multi-field earthkit response into per-chunk cache entries."""
        n_cells = 1
        for s in chunk_shape:
            n_cells *= s

        fields = list(data)

        # ── Time dimension: match fields by year/month metadata ─────────
        if self._batch_dim == "time" and dims[batch_pos] == "time":
            # Build lookup: (year, month) → time-axis index
            time_lookup = {}
            for bi in batch_indices:
                ts = pd.Timestamp(self._coords["time"][bi])
                time_lookup[(ts.year, ts.month)] = bi

            matched = set()
            for field in fields:
                try:
                    yr = int(field.metadata("year"))
                    mo = int(field.metadata("month"))
                except Exception:
                    continue
                key_tuple = (yr, mo)
                if key_tuple in time_lookup and key_tuple not in matched:
                    bi = time_lookup[key_tuple]
                    matched.add(key_tuple)
                    trial = list(indices)
                    trial[batch_pos] = bi
                    key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                    values = field.to_numpy().flatten().astype(np.float32)
                    if values.size == n_cells:
                        self._cache[key] = values.tobytes()
                    else:
                        self._cache[key] = np.full(
                            n_cells, np.nan, dtype=np.float32).tobytes()

            # Fill any unmatched batch indices with NaN
            for bi in batch_indices:
                trial = list(indices)
                trial[batch_pos] = bi
                key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                if key not in self._cache:
                    self._cache[key] = np.full(
                        n_cells, np.nan, dtype=np.float32).tobytes()
            return

        # ── Non-time dimensions: ordered matching ───────────────────────
        if len(fields) == len(batch_indices):
            for bi, field in zip(batch_indices, fields):
                trial = list(indices)
                trial[batch_pos] = bi
                key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                values = field.to_numpy().flatten().astype(np.float32)
                if values.size == n_cells:
                    self._cache[key] = values.tobytes()
                else:
                    self._cache[key] = np.full(
                        n_cells, np.nan, dtype=np.float32).tobytes()
        else:
            # Fallback: treat entire response as a single concatenated array
            all_values = data.to_numpy().flatten().astype(np.float32)
            expected = n_cells * len(batch_indices)
            if all_values.size == expected:
                for i, bi in enumerate(batch_indices):
                    trial = list(indices)
                    trial[batch_pos] = bi
                    key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                    chunk = all_values[i * n_cells:(i + 1) * n_cells]
                    self._cache[key] = chunk.tobytes()
            else:
                print(f"  ⚠ batch split: got {all_values.size} values, "
                      f"expected {expected} ({len(batch_indices)} x {n_cells})")
                for bi in batch_indices:
                    trial = list(indices)
                    trial[batch_pos] = bi
                    key = f"{var_name}/{self._chunk_key_for_indices(trial)}"
                    self._cache[key] = np.full(
                        n_cells, np.nan, dtype=np.float32).tobytes()

    # ── MutableMapping interface ────────────────────────────────────────

    def __getitem__(self, key):
        if key in self._kv:
            return self._kv[key]
        if key in self._cache:
            return self._cache[key]
        # Try to parse as a data variable chunk: "var_name/0.1.0"
        if "/" in key:
            parts = key.split("/", 1)
            var_name, chunk_key = parts[0], parts[1]
            if var_name in self._variables and "." in chunk_key:
                return self._fetch_chunk(var_name, chunk_key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        self._kv[key] = value

    def __delitem__(self, key):
        if key in self._kv:
            del self._kv[key]
        elif key in self._cache:
            del self._cache[key]
        else:
            raise KeyError(key)

    def __iter__(self):
        return iter(self._kv)

    def __len__(self):
        return len(self._kv)

    def __contains__(self, key):
        if key in self._kv:
            return True
        # Report data variable chunks as existing so zarr fetches them
        if "/" in key:
            parts = key.split("/", 1)
            if parts[0] in self._variables and "." in parts[1]:
                return True
        return False

    def listdir(self, prefix=""):
        """List entries under a prefix (used by zarr for discovery)."""
        if prefix:
            prefix = prefix.rstrip("/") + "/"
        entries = set()
        for k in self._kv:
            if k.startswith(prefix):
                rest = k[len(prefix):]
                entries.add(rest.split("/")[0])
        return sorted(entries)

    # ── Convenience ─────────────────────────────────────────────────────

    @property
    def batch_years(self):
        return self._batch_years

    @batch_years.setter
    def batch_years(self, value):
        self._batch_years = int(value)

    def open(self):
        """Open this store as an xarray Dataset (lazy — no data fetched).

        The returned Dataset (and its DataArrays) carry a reference to this
        store so that ``.polytope.sel()`` can auto-tune ``batch_years``.
        """
        import xarray as xr
        ds = xr.open_dataset(self, engine="zarr", consolidated=True)
        ds.attrs["_polytope_store"] = self
        for name in ds.data_vars:
            ds[name].attrs["_polytope_store"] = self
        return ds

    def clear_cache(self):
        """Free cached data chunks."""
        self._cache.clear()

    def __repr__(self):
        dims = ", ".join(f"{k}={v}" for k, v in self._dim_sizes.items())
        nvars = len(self._variables)
        return f"<PolytopeZarrStore {nvars} variables ({dims})>"


# ── xarray accessor: .polytope.sel() ────────────────────────────────────

def _infer_batch_years(store, sel_kwargs):
    """Set store.batch_years to span the requested time slice."""
    time_arg = sel_kwargs.get("time")
    if store is None or time_arg is None:
        return
    if isinstance(time_arg, slice):
        start = pd.Timestamp(time_arg.start) if time_arg.start else None
        stop = pd.Timestamp(time_arg.stop) if time_arg.stop else None
        if start is not None and stop is not None:
            store.batch_years = stop.year - start.year + 1


try:
    import xarray as _xr

    @_xr.register_dataarray_accessor("polytope")
    class _PolytopeDataArray:
        def __init__(self, da):
            self._da = da
            self._store = da.attrs.get("_polytope_store")

        def sel(self, **kwargs):
            """Like ``.sel()``, but auto-sets batch_years from the time slice."""
            _infer_batch_years(self._store, kwargs)
            return self._da.sel(**kwargs)

    @_xr.register_dataset_accessor("polytope")
    class _PolytopeDataset:
        def __init__(self, ds):
            self._ds = ds
            self._store = ds.attrs.get("_polytope_store")

        def sel(self, var=None, **kwargs):
            """Like ``.sel()``, but auto-sets batch_years from the time slice."""
            _infer_batch_years(self._store, kwargs)
            result = self._ds.sel(**kwargs)
            return result[var] if var is not None else result

except ImportError:
    pass
