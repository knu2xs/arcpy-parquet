# Feature Specification: PYT Parquet Import Selection

**Feature Branch**: `[001-pyt-parquet-import-selection]`  
**Created**: 2026-07-13  
**Status**: Draft  
**Input**: User description: "When an input parquet dataset is selected to import in the PYT, if the dataset is valid geoparquet, present the available geometry fields from the geoparquet as the available with the default geoparquet geometry field populated, and the spatial reference correctly populated, and if not specified defaulting to the WGS84 (4326) spatial reference. If not geoparquet, and the format is WGS, display parqet BYTE_ARRAY fields as available for selection (arrow reports these as BINARY or LARGE_BINARY). Last, if selecting coordinate columns, these must be one of FLOAT32 or FLOAT64 date type columns. Also, for the WGS or coordinate columns options, the spatial reference should default to WGS84 (4326)."

## Clarifications

### Session 2026-07-13

- Q: If GeoParquet has multiple geometry fields and no default, what should happen? → A: Do not auto-select any geometry field; require user selection.
- Q: If GeoParquet metadata is malformed/partial, what should happen? → A: Treat input as non-GeoParquet, allow WGS/coordinate modes with type-based validation, and show a warning in the UX.
- Q: How should user-selected spatial reference interact with defaults in WGS/coordinate modes? → A: User-selected spatial reference takes precedence; default to WGS84 only when spatial reference is unset.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Auto-detect GeoParquet geometry settings (Priority: P1)

As a GIS analyst importing a Parquet dataset through the PYT, I want the tool to detect valid GeoParquet metadata and show valid geometry fields with the default geometry field preselected so I can import without manual field discovery.

**Why this priority**: GeoParquet is the primary structured geospatial format for this workflow, and accurate field detection prevents failed imports and manual setup errors.

**Independent Test**: Can be fully tested by selecting a valid GeoParquet input and confirming geometry field choices, default geometry field selection, and populated spatial reference behavior without testing any non-GeoParquet path.

**Acceptance Scenarios**:

1. **Given** a selected input dataset with valid GeoParquet metadata and one geometry field marked as default, **When** the user opens geometry options in the PYT, **Then** the available list contains all valid GeoParquet geometry fields and preselects the metadata-defined default field.
2. **Given** a selected input dataset with valid GeoParquet metadata and explicit spatial reference metadata, **When** the tool populates import parameters, **Then** the spatial reference is set to the metadata-defined value.
3. **Given** a selected input dataset with valid GeoParquet metadata but no explicit spatial reference metadata, **When** the tool populates import parameters, **Then** the spatial reference defaults to WGS84 (EPSG:4326).

---

### User Story 2 - Support WGS binary geometry import mode (Priority: P2)

As a GIS analyst importing a non-GeoParquet dataset using WGS geometry mode, I want byte-array-style geometry fields shown as selectable candidates so I can choose the correct source geometry column.

**Why this priority**: Many non-GeoParquet datasets encode WGS geometry in binary-like Parquet column types, and users need clear compatible options to proceed.

**Independent Test**: Can be fully tested by selecting a non-GeoParquet dataset and WGS mode, then confirming only BYTE_ARRAY-compatible fields (including schema-reported BINARY and LARGE_BINARY) appear as valid choices and spatial reference defaults correctly.

**Acceptance Scenarios**:

1. **Given** a selected input dataset that is not valid GeoParquet and includes BYTE_ARRAY-compatible columns, **When** the user selects WGS mode, **Then** the selectable field list includes columns represented as BYTE_ARRAY, BINARY, or LARGE_BINARY.
2. **Given** a selected input dataset that is not valid GeoParquet and the user selects WGS mode, **When** spatial reference is populated, **Then** it defaults to WGS84 (EPSG:4326).
3. **Given** a selected input dataset that is not valid GeoParquet and the user has already selected a spatial reference, **When** WGS mode is selected, **Then** the user-selected spatial reference is preserved and not overwritten.

---

### User Story 3 - Enforce coordinate-column type constraints (Priority: P3)

As a GIS analyst using coordinate columns instead of encoded geometry, I want only numeric floating-point coordinate columns to be accepted so imports are spatially valid and predictable.

**Why this priority**: Restricting coordinate fields to FLOAT32/FLOAT64 avoids invalid geometry generation from non-numeric or imprecise source columns.

**Independent Test**: Can be fully tested by selecting coordinate-column mode and verifying accepted/rejected column types and default spatial reference behavior independently of GeoParquet and WGS binary modes.

**Acceptance Scenarios**:

1. **Given** a selected input dataset with mixed column types, **When** the user chooses coordinate-column mode, **Then** only FLOAT32 and FLOAT64 columns are offered for coordinate selection.
2. **Given** a selected input dataset and coordinate-column mode, **When** the user attempts to use a non-FLOAT32/FLOAT64 column as a coordinate input, **Then** the tool rejects the selection with a clear validation error.
3. **Given** a selected input dataset and coordinate-column mode, **When** spatial reference is populated, **Then** it defaults to WGS84 (EPSG:4326).
4. **Given** a selected input dataset and coordinate-column mode with an existing user-selected spatial reference, **When** parameters refresh, **Then** the existing user-selected spatial reference remains unchanged.

### Edge Cases

- A GeoParquet dataset contains multiple geometry fields but no default geometry designation; no geometry field is auto-selected and user selection is required.
- A GeoParquet dataset contains malformed or partial metadata that is insufficient for reliable geometry-field detection; the tool falls back to non-GeoParquet handling and displays a user-facing warning.
- A non-GeoParquet dataset in WGS mode has no BYTE_ARRAY/BINARY/LARGE_BINARY columns available.
- A coordinate-column workflow has only one valid floating-point column or mismatched X/Y candidate counts.
- A dataset with FLOAT32/FLOAT64 coordinate columns also includes null-heavy values in one coordinate field.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST evaluate the selected input Parquet dataset and determine whether it is valid GeoParquet before presenting geometry-source options.
- **FR-002**: When the selected dataset is valid GeoParquet, the system MUST present all detected GeoParquet geometry fields as available selection options.
- **FR-003**: When the selected dataset is valid GeoParquet and a default geometry field is defined, the system MUST preselect that default geometry field.
- **FR-003a**: When the selected dataset is valid GeoParquet with multiple geometry fields and no default geometry field designation, the system MUST not auto-select a geometry field and MUST require the user to select one before execution.
- **FR-004**: When the selected dataset is valid GeoParquet, the system MUST populate the spatial reference from dataset metadata when available.
- **FR-005**: When the selected dataset is valid GeoParquet but no spatial reference is specified, the system MUST default spatial reference to WGS84 (EPSG:4326).
- **FR-006**: When the selected dataset is not valid GeoParquet and WGS mode is selected, the system MUST present BYTE_ARRAY-compatible fields (including BINARY and LARGE_BINARY schema representations) as selectable geometry-source options.
- **FR-007**: When WGS mode is selected and spatial reference is unset, the system MUST default spatial reference to WGS84 (EPSG:4326).
- **FR-008**: When coordinate-column mode is selected, the system MUST allow coordinate field selection only from columns typed as FLOAT32 or FLOAT64.
- **FR-009**: The system MUST block coordinate-column selections that include non-FLOAT32/FLOAT64 fields and provide a clear validation message.
- **FR-010**: When coordinate-column mode is selected and spatial reference is unset, the system MUST default spatial reference to WGS84 (EPSG:4326).
- **FR-011**: If no compatible fields exist for the selected import mode, the system MUST prevent execution and display actionable guidance to select another mode or dataset.
- **FR-012**: When GeoParquet metadata is malformed or partial, the system MUST fall back to non-GeoParquet handling and continue to allow WGS mode and coordinate-column mode eligibility checks.
- **FR-013**: When fallback from malformed or partial GeoParquet metadata occurs, the system MUST display a warning message that explains fallback behavior and potential metadata quality issues.
- **FR-014**: User-selected spatial reference MUST take precedence over automatic defaulting in WGS mode and coordinate-column mode.

### Constitution Alignment Requirements *(mandatory)*

- **CA-001**: The feature MUST define implementation tasks that preserve PEP8 compliance, explicit type hints, and Google-style docstrings for all changed Python functions.
- **CA-002**: The feature MUST define automated tests in `testing/` for GeoParquet detection, WGS binary-field eligibility, coordinate-column type validation, and spatial-reference defaulting behavior.
- **CA-003**: If ArcPy data update logic is introduced while implementing this feature, the implementation MUST prefer `arcpy.da.UpdateCursor` and document any intermediate workspace strategy.
- **CA-004**: The feature MUST include updates to relevant documentation and workflow artifacts when user-visible PYT parameter behavior changes.
- **CA-005**: The feature MUST confirm that no credentials or secrets are introduced into tracked source, tests, or specs and that configuration remains in approved locations.

### Key Entities *(include if feature involves data)*

- **Input Parquet Dataset**: A user-selected dataset whose metadata and column schema determine available import parameter choices.
- **Import Mode**: The selected parsing path that controls field eligibility and defaults (GeoParquet geometry, WGS binary geometry, or coordinate columns).
- **Geometry Field Candidate**: A column eligible for geometry import under the current mode, including GeoParquet geometry fields and BYTE_ARRAY-compatible WGS fields.
- **Coordinate Field Candidate**: A numeric column eligible for X/Y coordinate selection, constrained to FLOAT32 or FLOAT64 types.
- **Spatial Reference Selection**: The resolved coordinate system value for import, sourced from valid GeoParquet metadata when present or defaulted to WGS84 (EPSG:4326).

## Assumptions

- The PYT parameter UI updates dynamically after dataset selection and mode changes.
- Dataset schema/type metadata is available at parameter-validation time.
- WGS mode refers to geometry encoded in binary-compatible Parquet fields for geographic coordinates in EPSG:4326.
- Existing import modes remain unchanged except for the field-eligibility and defaulting behaviors defined in this specification.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In UAT samples of valid GeoParquet inputs, 100% of runs show all eligible geometry fields and preselect the metadata-defined default geometry field when provided.
- **SC-002**: In UAT samples where GeoParquet spatial reference metadata is missing, 100% of runs default spatial reference to WGS84 (EPSG:4326).
- **SC-003**: In UAT samples of non-GeoParquet datasets using WGS mode, 100% of eligible BYTE_ARRAY/BINARY/LARGE_BINARY fields are presented and 0 ineligible fields are presented.
- **SC-004**: In coordinate-column mode validation tests, 100% of non-FLOAT32/FLOAT64 coordinate field selections are blocked before execution.
- **SC-005**: At least 95% of test users can configure a valid import mode and field selection on the first attempt for each of the three modes (GeoParquet, WGS binary, coordinate columns).
