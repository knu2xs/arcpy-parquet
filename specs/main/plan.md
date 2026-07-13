# Implementation Plan: Efficient GeoParquet Integration

**Branch**: `main` | **Date**: 2026-03-08 | **Spec**: `specs/main/spec.md`
**Input**: Feature specification from `specs/main/spec.md`

## Summary

Integrate the efficient GeoParquet conversion implementation into
`arcpy_parquet` while preserving backward compatibility and ArcGIS toolbox
workflows. The technical approach centers on batch-streamed Arrow RecordBatch
processing, explicit public API exports, compatibility wrappers with deprecation
guidance, and documentation/test updates aligned with GeoParquet 1.1 behavior.

## Technical Context

**Language/Version**: Python 3.9+  
**Primary Dependencies**: arcpy, pyarrow, pandas, numpy  
**Storage**: File geodatabases (`.gdb`) and file/directory-based Parquet datasets  
**Testing**: pytest (unit + integration in `testing/`)  
**Target Platform**: ArcGIS Pro Python environment on Windows
**Project Type**: Python library + ArcGIS Python toolbox integration  
**Performance Goals**: Reduce peak memory usage for large conversions through batch streaming and support large datasets via partitioned output  
**Constraints**: Preserve legacy API compatibility during migration; keep GeoParquet 1.1 metadata correctness; avoid introducing new secret/configuration risks  
**Scale/Scope**: Package exports, conversion module migration, toolbox parameter mapping, tests, and docs updates across repository-maintained workflows

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Design Gate Review

- [x] Code quality gate: Planned modules and touched files enforce PEP8,
  explicit type hints, and Google-style docstrings.
- [x] Testability gate: Plan includes automated test additions for conversion
  behavior, schema/metadata handling, and compatibility behavior.
- [x] ArcPy performance gate: Design preserves batch/streaming behavior and
  prefers efficient cursor/memory-safe geoprocessing patterns.
- [x] Reproducibility gate: Plan explicitly includes updates for docs/scripts and
  repo workflows (`make test`, docs paths, toolbox integration).
- [x] Security gate: No secrets required; config and sensitive values remain in
  `config/secrets.ini` or external environment channels.

### Post-Design Gate Review

- [x] Phase 0 research resolved all uncertainties without unresolved
  clarifications.
- [x] Phase 1 artifacts (`data-model.md`, `contracts/`, `quickstart.md`) include
  testable interface and validation flow details aligned to constitution.

## Project Structure

### Documentation (this feature)

```text
specs/main/
├── plan.md
├── spec.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── geoparquet-api-contract.md
└── tasks.md
```

### Source Code (repository root)

```text
src/
├── arcpy_parquet/
│   ├── __init__.py
│   ├── __main__.py
│   ├── geoparquet.py
│   └── utils/
│       ├── __init__.py
│       ├── logging_utils.py
│       └── main.py
└── parquet/
    └── _parquet.py

arcgis/
└── ArcPy-Parquet-Tools.pyt

testing/
├── test_arcpy_parquet.py
├── test_parquet_to_feature_class.py
└── test_geoparquet_validity.py
```

**Structure Decision**: Use existing single-project Python package layout with
ArcGIS toolbox integration and repository-local testing/documentation paths.

## Complexity Tracking

No constitution violations identified; no complexity exemptions required.
