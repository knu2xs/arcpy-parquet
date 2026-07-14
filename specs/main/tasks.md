# Tasks: Efficient GeoParquet Integration

**Input**: Design documents from `specs/main/`
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/geoparquet-api-contract.md`, `quickstart.md`

**Tests**: This feature changes conversion behavior and schema/metadata handling. Automated tests are required and included per story.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare module layout and baseline quality updates needed by all stories.

- [X] T001 Create module scaffold files `src/arcpy_parquet/geoparquet.py` and `src/arcpy_parquet/_compat.py`.
- [X] T002 [P] Add utility support for filename-safe naming in `src/arcpy_parquet/utils/main.py` and export updates in `src/arcpy_parquet/utils/__init__.py`.
- [X] T003 [P] Add test module scaffolds `testing/test_geoparquet_api.py`, `testing/test_geoparquet_roundtrip.py`, and `testing/test_compat_deprecations.py`.
- [X] T004 [P] Document test fixture assumptions for new conversion tests in `testing/README_TESTS.md`.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Implement common behavior that all user stories depend on.

**CRITICAL**: No user story work starts until this phase is complete.

- [X] T005 Implement shared conversion config and validation helpers in `src/arcpy_parquet/geoparquet.py`.
- [X] T006 [P] Implement GeoParquet metadata extraction and geometry-column discovery helpers in `src/arcpy_parquet/geoparquet.py`.
- [X] T007 [P] Implement shared error messages/exceptions for path validation, overwrite behavior, and geometry-column validation in `src/arcpy_parquet/geoparquet.py`.
- [X] T008 Implement compatibility wrapper helpers and deprecation warning utilities in `src/arcpy_parquet/_compat.py`.
- [X] T009 Wire package-level exports for new modules in `src/arcpy_parquet/__init__.py`.
- [X] T010 Define baseline test fixtures/utilities reused across stories in `testing/conftest.py`.

**Checkpoint**: Foundation complete. User stories can now be delivered independently.

---

## Phase 3: User Story 1 - Integrate New GeoParquet API (Priority: P1) 🎯 MVP

**Goal**: Deliver stable public GeoParquet API for export/import with compliant metadata and geometry-column handling.

**Independent Test**: Execute `features_to_geoparquet` and `geoparquet_to_features` on representative sample data and verify metadata, geometry integrity, and successful roundtrip.

### Tests for User Story 1

- [ ] T011 [P] [US1] Add API contract tests for `features_to_geoparquet`, `geoparquet_to_features`, and `get_geometry_columns` in `testing/test_geoparquet_api.py`.
- [ ] T012 [P] [US1] Add GeoParquet metadata validity assertions (version, primary column, geometry encoding, CRS) in `testing/test_geoparquet_validity.py`.
- [ ] T013 [P] [US1] Add export/import roundtrip integration test coverage in `testing/test_geoparquet_roundtrip.py`.
- [ ] T014 [P] [US1] Add partitioned export behavior tests for Hive-style layout and invalid/empty partition values in `testing/test_geoparquet_roundtrip.py`.
- [ ] T015 [P] [US1] Add configurable batch-size behavior tests including boundary values and failure cases in `testing/test_geoparquet_api.py`.

### Implementation for User Story 1

- [X] T016 [US1] Implement `features_to_parquet` with GeoParquet metadata writing (`GEOPARQUET`, WKB internally) in `src/arcpy_parquet/__main__.py`.
- [X] T017 [US1] Implement `geoparquet_to_features` with primary/explicit geometry-column selection in `src/arcpy_parquet/geoparquet.py`.
- [X] T018 [US1] Implement `get_geometry_columns` deterministic output behavior in `src/arcpy_parquet/geoparquet.py`.
- [X] T019 [US1] Implement explicit partition value normalization and deterministic fallback behavior in `src/arcpy_parquet/geoparquet.py`.
- [X] T020 [US1] Implement batch-size validation and error messaging in `src/arcpy_parquet/geoparquet.py`.
- [X] T021 [US1] Add package exports for US1 functions in `src/arcpy_parquet/__init__.py`.
- [X] T022 [US1] Add/refresh function-level Google-style docstrings and type hints for public API signatures in `src/arcpy_parquet/geoparquet.py`.
- [X] T023 [US1] Remove obsolete direct public exposure from `src/parquet/__init__.py` by redirecting callers to `src/arcpy_parquet/geoparquet.py`.

**Checkpoint**: User Story 1 is independently functional and testable (MVP).

---

## Phase 4: User Story 2 - Preserve Existing Consumer Workflows (Priority: P2)

**Goal**: Keep legacy API/toolbox workflows working while moving implementation to the new GeoParquet API.

**Independent Test**: Run legacy entry points and toolbox execution paths, verifying functional outputs and deprecation warnings.

### Tests for User Story 2

- [X] T024 [P] [US2] Add deprecation warning and behavior-preservation tests for legacy public functions in `testing/test_compat_deprecations.py`.
- [ ] T025 [P] [US2] Add regression coverage for legacy conversion entry points in `testing/test_arcpy_parquet.py`.
- [ ] T026 [P] [US2] Add regression coverage for import behavior with explicit geometry column and overwrite settings in `testing/test_parquet_to_feature_class.py`.

### Implementation for User Story 2

- [ ] T027 [US2] Refactor legacy public functions to call new API with warnings in `src/arcpy_parquet/__main__.py`.
- [X] T028 [US2] Implement compatibility routing helpers for legacy signatures in `src/arcpy_parquet/_compat.py`.
- [ ] T029 [US2] Update toolbox import/export tool execution mapping to new API functions in `arcgis/ArcPy-Parquet-Tools.pyt`.
- [ ] T030 [US2] Update toolbox XML parameter docs for added/changed arguments in `arcgis/ArcPy-Parquet-Tools.FeatureClassToParquet.pyt.xml` and `arcgis/ArcPy-Parquet-Tools.GeoparquetToFeatureClass.pyt.xml`.

**Checkpoint**: User Story 2 remains independently testable with backward-compatible behavior.

---

## Phase 5: User Story 3 - Document and Validate Migration (Priority: P3)

**Goal**: Provide migration documentation and reproducible validation steps for maintainers.

**Independent Test**: Follow quickstart and documentation steps from a clean environment and confirm all referenced commands and examples execute successfully.

### Tests for User Story 3

- [ ] T031 [P] [US3] Add quickstart command verification test coverage for documented API imports and smoke scenarios in `testing/test_geoparquet_api.py`.
- [ ] T032 [P] [US3] Add regression assertions for documented migration examples in `testing/test_compat_deprecations.py`.

### Implementation for User Story 3

- [X] T033 [US3] Update migration guidance and API examples in `README.md`.
- [X] T034 [US3] Update toolbox migration and packaging notes in `arcgis/README.md`.
- [X] T035 [US3] Update feature quickstart validation steps in `specs/main/quickstart.md`.
- [X] T036 [US3] Update docs source content for new API and compatibility lifecycle in `docsrc/mkdocs/index.md`.

**Checkpoint**: User Story 3 is independently testable through documentation-led validation.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final consistency, governance, and reproducibility validation across all stories.

- [ ] T037 [P] Run full conversion test suite and fix any cross-story regressions in `testing/test_arcpy_parquet.py`, `testing/test_parquet_to_feature_class.py`, and `testing/test_geoparquet_validity.py`.
- [ ] T038 [P] Validate package exports and import paths consistency in `src/arcpy_parquet/__init__.py` and `src/parquet/__init__.py`.
- [ ] T039 Validate make/docs reproducibility paths for this feature in `README.md`, `arcgis/README.md`, and `docsrc/mkdocs.yml`.
- [ ] T040 Perform tracked-file config/security review for this feature scope in `config/config.ini` and `config/secrets.ini`.
- [ ] T041 Execute quickstart end-to-end and record final validation notes in `specs/main/quickstart.md`.
- [ ] T042 Measure touched-module coverage and enforce minimum 80% threshold for SC-002 using the existing pytest coverage workflow, with results recorded in CI output.

---

## Dependencies & Execution Order

### Phase Dependencies

- Setup (Phase 1): starts immediately.
- Foundational (Phase 2): depends on Setup completion; blocks all user stories.
- User Story phases (Phase 3-5): depend on Foundational completion; can proceed in parallel when staffed.
- Polish (Phase 6): depends on completion of all targeted user stories.

### User Story Dependencies

- US1 (P1): starts after Phase 2; no dependency on US2/US3.
- US2 (P2): starts after Phase 2; depends on US1 APIs existing but remains independently testable.
- US3 (P3): starts after Phase 2; depends on stable API/compat behavior from US1/US2 outputs.

### Dependency Graph

- US1 -> US2 -> US3 for incremental delivery.
- US1 alone is the suggested MVP scope.
- US2 and US3 can run in parallel after US1 interface stabilization if team capacity supports it.

---

## Parallel Execution Examples

### User Story 1

- Run T011, T012, T013, T014, and T015 concurrently because they touch different test files.

### User Story 2

- Run T024, T025, and T026 concurrently because they target separate regression suites.

### User Story 3

- Run T031 and T032 concurrently because quickstart and migration regressions are isolated.

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1 and Phase 2.
2. Deliver Phase 3 (US1).
3. Validate US1 independently via API contract, metadata validity, and roundtrip tests.
4. Demo or release MVP.

### Incremental Delivery

1. Deliver US1 (core new API).
2. Deliver US2 (backward compatibility and toolbox mapping).
3. Deliver US3 (migration docs and validation).
4. Finish with Phase 6 polish.

### Parallel Team Strategy

1. Team completes Setup + Foundational together.
2. After Phase 2:
   - Developer A leads US1 implementation.
   - Developer B prepares US2 test scaffolding and compatibility tests.
   - Developer C prepares US3 documentation updates.
3. Merge sequentially by dependency, then run Phase 6 cross-cutting validation.
