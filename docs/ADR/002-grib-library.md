# ADR 002: GRIB Library (grib2io → pygrib)

## Status

Accepted

## Context

Jackfruit's L2 transformation layer decodes CAMS Europe GRIB files — specifically Product Definition Template 4.40 (atmospheric chemical constituents) — to extract grid data for storage in ClickHouse. The initial GRIB library choice was **grib2io**, which uses NOAA's g2clib as its backend.

Over time, grib2io accumulated significant technical debt:

1. **No native PDT 4.40 support** — required a manual patch module (`pdt40.py`, 94 lines of monkey-patching) to decode atmospheric chemical constituent messages
2. **No constituent code → variable name mapping** — required a manual lookup table (`shortnames.py`) to translate GRIB parameter codes to human-readable names
3. **Heavy system dependencies** — `libg2c0`, `libg2c-dev`, `build-essential`, and `gcc` were required in the Docker image for the C backend
4. **Circular import bug** in v2.7.0 forced a pin to v2.6.0, with unclear maintenance trajectory

The **catalyst** for migration was a data-correctness bug: CAMS Europe's grid crosses the prime meridian, with longitudes spanning 335°E to 45°E. grib2io's linspace computation for this grid type produced longitudes in the range −335° to 45° instead of the correct −24.95° to 44.95°. Every coordinate stored in ClickHouse was wrong. This was not a DX annoyance — it was a fundamental data-integrity issue.

This experience made it clear that patching around grib2io was not sustainable, and that different GRIB-producing agencies (ECMWF, NOAA, DWD) have different quirks. The pipeline needs the ability to swap GRIB libraries per data source rather than committing to one library for all sources.

### Options Considered

**Option A: Stay with grib2io** — patch around the gaps

- ✅ Already working (kinda)
- ❌ Longitude bug for prime-meridian-crossing grids — data correctness issue
- ❌ Maintaining custom PDT 4.40 patch is fragile
- ❌ Pinned to v2.6.0, unclear maintenance trajectory
- ❌ Heavy system dependencies in Docker image

**Option B: Use ecCodes directly** — ECMWF's C library with Python bindings (`eccodes`)

- ✅ Full control over decoding — access to every key and metadata field
- ✅ Same underlying library that pygrib wraps
- ❌ Low-level API — manual message iteration, key lookup by string, explicit memory management (`codes_release`)
- ❌ Verbose boilerplate for routine operations (extracting lats/lons/values requires multiple calls)
- ❌ Easy to introduce bugs from mishandled message lifecycles

**Option C: Migrate to pygrib (ecCodes)** — high-level wrapper around ecCodes

- ✅ Native PDT 4.40 and constituent code support (ecCodes definitions)
- ✅ `message.data()` returns `(values, lats, lons)` — perfect fit for GridData
- ✅ Ships as a wheel with bundled ecCodes — no system dependencies
- ✅ Actively maintained by ECMWF (the source of our data)
- ⚠️ Still no constituent code → variable name mapping — must be handled manually in the adapter

## Decision

Migrate to **pygrib** and introduce a Protocol-based reader abstraction (`GribReader` / `GribMessage`) so that asset code never imports the GRIB library directly.

The abstraction exists because different data sources (ECMWF CAMS, GloFAS, ERA5, potentially DWD ICON-EU) may need different GRIB libraries or adapter logic — each gets its own adapter implementing the same Protocol. This mirrors the `GridStore` pattern from [ADR 001](001-grid-data-storage.md).

## Consequences

### Positive

- **Longitude bug fixed** — coordinates now correct for prime-meridian-crossing grids
- **Deleted ~3 workaround modules** (`pdt40.py`, `shortnames.py`, `grib_sanity_check.py`)
- **Docker image smaller** — removed 4 system packages (`libg2c0`, `libg2c-dev`, `build-essential`, `gcc`)
- **Reader Protocol** decouples assets from GRIB library — future data sources get their own adapter
- **Python version constraint relaxed** to >=3.14

### Negative

- Constituent code → variable name mapping still manual (carried over from grib2io, now lives in the adapter)

### Mitigations

- **Protocol abstraction** (`GribReader` / `GribMessage`) isolates pygrib-specific knowledge to `adapters/cams_adapter.py`
- **Comprehensive test suite** validates adapter behavior against real GRIB fixture

## References

- Protocol definitions: `src/pipeline_python/grib2/reader.py`
- CAMS adapter: `src/pipeline_python/grib2/adapters/cams_adapter.py`
- Asset consumption: `src/pipeline_python/defs/assets.py` (`transform_cams_data`)
- Tests: `tests/unit/test_grib2/test_cams_adapter.py`
- PR: https://github.com/kacper-wojtaszczyk/jackfruit/pull/68
