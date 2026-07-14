# Research: Efficient GeoParquet Integration

## Decision 1: Integration Surface Includes Python API and ArcGIS Toolbox

- Decision: Deliver feature work across both `src/arcpy_parquet/` public APIs and `arcgis/ArcPy-Parquet-Tools.pyt` parameter mapping.
- Rationale: The feature requires parity for Python callers and toolbox users; partial delivery would create workflow mismatch.
- Alternatives considered: Limit scope to package API only. Rejected due to missing toolbox compatibility.

## Decision 2: Standardize on Streaming RecordBatch Conversion

- Decision: Use Arrow RecordBatch streaming patterns for export and import paths.
- Rationale: Streaming minimizes memory pressure and aligns with the efficiency objective for large geospatial datasets.
- Alternatives considered: Full-table DataFrame loading. Rejected due to higher memory use and weaker scalability.

## Decision 3: Preserve Compatibility with Deprecation Wrappers

- Decision: Keep legacy conversion entry points callable and emit explicit migration warnings.
- Rationale: Existing users need non-breaking transition behavior while new APIs are adopted.
- Alternatives considered: Immediate removal of legacy functions. Rejected due to avoidable breaking change risk.

## Decision 4: Enforce GeoParquet 1.1 Metadata Contract

- Decision: Treat GeoParquet output (with internal WKB geometry encoding) and required GeoParquet metadata keys as non-negotiable guarantees.
- Rationale: Interoperability and downstream correctness depend on metadata compliance.
- Alternatives considered: Best-effort metadata without strict contract checks. Rejected because it risks invalid outputs.

## Decision 5: Test Strategy Centers on Pytest Integration Coverage

- Decision: Add/maintain pytest coverage for export/import paths, compatibility behavior, schema handling, and failure modes.
- Rationale: Current repository tooling and quality gates are already standardized on pytest in `testing/`.
- Alternatives considered: Split test frameworks by subsystem. Rejected due to overhead and reduced consistency.

## Decision 6: Documentation and Workflow Updates Ship with Code

- Decision: Update user-facing docs and keep make-driven workflows reproducible in the same feature lifecycle.
- Rationale: Constitution requires reproducibility and synchronized docs/tooling changes.
- Alternatives considered: Delay docs updates until later release cycle. Rejected because it creates migration ambiguity.

## Resolved Clarifications

- No unresolved `NEEDS CLARIFICATION` items remain in technical context.
- Planning assumptions are concrete enough to proceed to task generation.
