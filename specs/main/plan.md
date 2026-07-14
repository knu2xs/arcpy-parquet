# Implementation Plan: Efficient GeoParquet Integration

**Branch**: `main` | **Date**: 2026-07-13 | **Spec**: `specs/main/spec.md`
**Input**: Feature specification from `specs/main/spec.md`

## Summary

Integrate the efficient GeoParquet conversion implementation into the public `arcpy_parquet` package and ArcGIS Python toolbox while preserving legacy entry points through deprecation wrappers. The approach standardizes on streaming Arrow batch conversion, explicit GeoParquet metadata handling, toolbox parameter remapping, and test/documentation updates that preserve existing workflows.

## Technical Context

**Language/Version**: Python >=3.9 (project requires `>=3.9`; ArcGIS Pro-managed environment)  
**Primary Dependencies**: `arcpy`, `pyarrow`, `pandas`, `numpy`, `arcgis`  
**Storage**: File-based datasets (Parquet/GeoParquet files and directories, file geodatabases)  
**Testing**: `pytest` in `testing/`  
**Target Platform**: Windows ArcGIS Pro Python environment (Conda-managed)  
**Project Type**: Python library + ArcGIS Python toolbox integration  
**Performance Goals**: Batch-streamed conversion that avoids full-table loads; practical parity or improvement versus legacy conversion on representative datasets  
**Constraints**: Preserve backward compatibility with deprecation warnings; maintain GeoParquet 1.1 metadata correctness; avoid secrets in tracked files  
**Scale/Scope**: Repository-scoped feature touching package API, toolbox parameter mapping, tests, and docs

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Research Gate Check

- [x] Code quality gate: Planned code changes satisfy PEP8, type hints, and Google-style docstring requirements.
- [x] Testability gate: Plan defines automated test updates in `testing/` for any behavior, schema, or conversion-path changes.
- [x] ArcPy performance gate: ArcPy update workflows use `arcpy.da.UpdateCursor` where update cursors are required; no deviation expected for this conversion-centric feature.
- [x] Reproducibility gate: Workflow/tooling impacts include coordinated updates to make/script/docs paths when behavior changes.
- [x] Security gate: No secret values are introduced in tracked source files; config handling remains in `config/secrets.ini` or secure environment channels.

### Post-Design Re-check

- [x] Design artifacts include explicit quality, testing, and compatibility coverage.
- [x] Data model and contracts define conversion-path behavior and error conditions.
- [x] Quickstart includes reproducible validation steps aligned with existing project workflows.
- [x] No constitution violations require complexity exceptions.

## Project Structure

### Documentation (this feature)

```text
specs/main/
+-- plan.md
+-- research.md
+-- data-model.md
+-- quickstart.md
+-- contracts/
|   +-- geoparquet-api-contract.md
+-- tasks.md
```

### Source Code (repository root)

```text
src/
+-- arcpy_parquet/

arcgis/
+-- ArcPy-Parquet-Tools.pyt

testing/
+-- test_arcpy_parquet.py
+-- test_parquet_to_feature_class.py
+-- test_geoparquet_validity.py

scripts/
+-- make_pyt_archive.py

docsrc/
+-- mkdocs.yml

docs/
+-- README.md

arcgis/
+-- README.md
```

**Structure Decision**: Use the existing single-package repository layout, adding/adjusting behavior in `src/arcpy_parquet/` and `arcgis/ArcPy-Parquet-Tools.pyt`, with verification in `testing/` and migration guidance in docs.

## Phase 0: Research Output

Research completed in `specs/main/research.md` with all technical context resolved and no open clarifications.

## Phase 1: Design Output

Design completed with:
- `specs/main/data-model.md`
- `specs/main/contracts/geoparquet-api-contract.md`
- `specs/main/quickstart.md`

## Complexity Tracking

No constitution violations identified; no exceptions required.
