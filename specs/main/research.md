# Research: Efficient GeoParquet Integration

## Decision 1: Treat implementation as Python library + ArcGIS toolbox integration

- Decision: Plan and execute integration as a hybrid project: Python package changes in `src/arcpy_parquet/` plus ArcGIS toolbox updates in `arcgis/ArcPy-Parquet-Tools.pyt`.
- Rationale: The feature requirements and task inventory explicitly target both API consumers and toolbox users.
- Alternatives considered: Restrict scope to package-only integration. Rejected because it leaves first-party toolbox workflows inconsistent.

## Decision 2: Use batch-streamed Arrow dataset reads/writes as the canonical conversion pattern

- Decision: Keep RecordBatch-based streaming as the required implementation pattern for export/import.
- Rationale: This pattern is already reflected in the incoming implementation goals (memory efficiency, scalability) and aligns with project constraints.
- Alternatives considered: Full-table reads with pandas/geopandas. Rejected due to higher memory pressure and weaker alignment with large dataset workflows.

## Decision 3: Preserve backward compatibility through deprecation wrappers

- Decision: Maintain legacy entry points while issuing clear deprecation warnings and migration targets.
- Rationale: Constitution and existing user workflows require low-risk migration rather than abrupt API removal.
- Alternatives considered: Immediate removal of legacy functions. Rejected because it introduces avoidable breaking changes.

## Decision 4: Contract surface includes Python API plus ArcGIS toolbox parameter mapping

- Decision: Define explicit contracts for Python-callable signatures and toolbox parameter translation.
- Rationale: External interfaces exist in both contexts and need stable, testable expectations.
- Alternatives considered: Document only internal implementation details. Rejected because it does not constrain user-facing behavior.

## Decision 5: Keep test strategy centered on pytest with integration-heavy coverage

- Decision: Use pytest for unit/integration/compatibility tests in `testing/`, including GeoParquet metadata and roundtrip scenarios.
- Rationale: Current repository testing stack and documentation already depend on pytest.
- Alternatives considered: Split test frameworks by component. Rejected due to complexity and reduced maintainability.

## Decision 6: No unresolved technical clarifications remain for planning

- Decision: Mark all technical context fields with concrete values in plan artifacts.
- Rationale: Inputs from repository structure, pyproject dependencies, and imported tasks are sufficient.
- Alternatives considered: Deferring context values as unresolved placeholders. Rejected to satisfy plan workflow requirement for resolved clarifications.
