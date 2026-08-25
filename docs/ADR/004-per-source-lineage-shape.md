# ADR 004: Per-Source Lineage Shape

## Status

Proposed

## Context

The Jackfruit serving API returns environmental data for `(lat, lon, timestamp)` queries against gridded data in ClickHouse, enriched with lineage metadata from Postgres (`catalog.curated_data` joined to `catalog.raw_files`). The original lineage shape was a singleton `{source, dataset, raw_file_id}` per variable, on the assumption that each returned value corresponds to a single stored row identified by `catalog_id`.

Two forcing functions break the singleton assumption:

1. **Temporal interpolation (JF-06).** The redesigned spatiotemporal matching computes returned values as linear interpolations between two bracketing timestamps. Every non-exact-match response now has at least two raw rows contributing to its value. A singleton lineage cannot honestly describe this.

2. **Licensing compliance.** Both CAMS (Copernicus License rev. 12) and ECMWF Open Data (CC BY 4.0 + ECMWF Terms) require source attribution at the user-facing distribution surface. The obligation is owed *per source*, not per cell. The pre-JF-06 response carried no attribution at all — a compliance gap that JF-06 also addresses.

A third concern, surfaced during scoping but explicitly de-scoped from JF-06, is multi-source-per-variable (backlog item JF-F1 — DWD ICON-EU as a second source for `temperature`). The lineage shape chosen here must accommodate the multi-source case when it lands, without further contract change.

### Options Considered

**Option A: Singleton lineage (status quo)**

Keep `{source, dataset, raw_file_id}` per variable. Pick one of the contributing rows as the "representative."

- ✅ No contract change
- ❌ Cannot honestly describe interpolated values; loses traceability to the second contributing row
- ❌ Doesn't carry attribution metadata — compliance gap remains

**Option B: Per-cell lineage with embedded attribution**

The `lineage` field becomes a list of cell-level entries: `[{catalog_id, ref_timestamp, actual_lat, actual_lon, attribution_block}, ...]`.

- ✅ Honest about interpolation
- ✅ Carries attribution
- ⚠️ Duplicates the full `AttributionBlock` for every entry — for 5 variables × 2 contributors sharing one source, the response carries 10 identical attribution blocks
- ⚠️ FE rendering must dedup repeated attribution at display time
- ⚠️ Per-cell granularity doesn't match the per-source granularity of the legal obligation

**Option C: Per-source lineage with contributors**

The `lineage` field is a list of per-source entries, each carrying one `AttributionBlock` and a list of contributors (the rows from that source).

- ✅ Honest about interpolation — every contributing cell appears in `contributors`
- ✅ Attribution at the granularity legally owed
- ✅ No duplication when cells share a source
- ✅ FE rendering: one attribution block per source per variable, no dedup logic
- ✅ Multi-source-per-variable (JF-F1) is a natural fit — list grows from length-1 to length-N
- ⚠️ Contract change requires lockstep updates in `buttprint-api` and `buttprint-fe`

**Option D: Top-level deduped `attributions` block + per-cell lineage**

A response-level `attributions: {source_key: AttributionBlock, ...}` block, separate from per-variable `lineage` arrays that reference sources by key.

- ✅ Maximum wire-size compactness
- ❌ Two structures to keep coherent — a stale key on a lineage entry would silently fail compliance
- ❌ Less self-contained — consumers must cross-reference to render any single entry
- ❌ Premature optimization at current response sizes (≤2 sources, ≤5 variables)

## Decision

Use **Option C: per-source lineage with contributors**.

The Go domain types:

```go
type Lineage struct {
    Source       string             // source key matching Postgres catalog.raw_files.source
    Attribution  AttributionBlock   // one per source, with [YEAR] filled at response time
    Contributors []ContributorRow   // the rows from this source that contributed
}

type ContributorRow struct {
    CatalogID    uuid.UUID          // references catalog.curated_data.id
    RefTimestamp time.Time          // the stored row's timestamp
    ActualLat    float32
    ActualLon    float32
}
```

Per-variable response carries `lineage []Lineage` (always non-empty for successful responses). For today's single-source-per-variable reality, each list is length-1, and `Contributors` is length-1 (exact-match-on-latest case) or length-2 (interpolation pair).

`AttributionBlock` carries `source`, `copyright` (with `[YEAR]` filled at response time using the *latest* contributor's `RefTimestamp.UTC().Year()` — keeps one `AttributionBlock` per source even when contributors span a year boundary), `license`, `license_url`, and `disclaimer` (where mandated).

This shape applies to the new `/v1/environmental/point` endpoint introduced in JF-06. The old `/v1/environmental` retains its singleton-lineage contract until cutover, then is removed within the same milestone.

## Consequences

### Positive

- **Honest about interpolation.** Every contributing cell is named in `Contributors`. Downstream consumers can compute the interpolation weight α from contributor timestamps and per-variable `applies_to_timestamp` without server-side metadata.
- **Attribution at the granularity legally owed.** No wire-size duplication when cells share a source.
- **Forward-compatible with multi-source-per-variable.** When JF-F1 lands, the lineage list grows from length-1 to length-N without further contract change.
- **Simpler FE rendering.** No client-side dedup logic needed; the per-source shape already dedups at the variable level.
- **Compliance gap closed.** Attribution is structurally mandatory rather than absent.

### Negative

- **Contract break, lockstep updates required.** `buttprint-api` (API-08) and `buttprint-fe` (FE-11) adopt the new shape in coordination with this change.
- **Per-variable lineage is no longer an optional singleton.** Consumers cannot treat lineage as a flat optional field — the structure is mandatory and nested.
- **Per-cell granularity available only via `Contributors`.** Consumers reasoning about which cell contributed at which timestamp must inspect the contributors list rather than reading flat `actual_lat`/`actual_lon` at the variable level.
- **ADR 001 unchanged but narrower in role.** The Postgres join via `catalog_id` still materializes lineage; now each query may resolve multiple `catalog_id` values rather than one. No structural change to ADR 001.

### Mitigations

- **Lockstep updates planned.** API-08 and FE-11 land in the same milestone as JF-06; the URL split gives a clean cutover window rather than forcing a single atomic flip.
- **Hardcoded Go source-spec map (not Postgres-backed yet).** The per-source `AttributionBlock` lives in `var sources = map[string]sourceSpec{...}` in Go code, not a `catalog.source` Postgres table. License strings change rarely; the migration is deferred until there are >5 sources or license strings begin changing.
- **Lineage failures fail the request.** No fallback path emitting partial/static-attribution responses — the goal is contract honesty, not best-effort delivery.

## References

- JF-06 (rescoped): spatiotemporal matching + lineage/compliance redesign.
- API-08: `buttprint-api` adoption of the new lineage shape.
- FE-11: `buttprint-fe` adoption of the new lineage shape.
- JF-F1 (backlog): multi-source-per-variable disambiguation — accommodated by this shape without further contract change.
- [ADR 001](001-grid-data-storage.md) — Grid data storage abstraction. Unchanged; `catalog_id` join still materializes lineage.
- Domain types (post-JF-06): `serving-go/internal/domain/lineage.go` (`Lineage`, `ContributorRow`).
- Source spec (new package, post-JF-06): `serving-go/internal/source/spec.go` (`AttributionBlock`, `sourceSpec`).
- JSON wire shape (post-JF-06): `serving-go/internal/api/response.go`.
