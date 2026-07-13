# Data Model: Efficient GeoParquet Integration

## Entity: ConversionJob

- Purpose: Encapsulates runtime configuration for export/import operations.
- Fields:
  - `job_id` (string): Unique identifier for traceability.
  - `operation` (enum): `export` or `import`.
  - `source_path` (string): Input feature class path or GeoParquet dataset path.
  - `target_path` (string): Output dataset path or feature class path.
  - `fields` (list[string] | null): Selected attributes for export.
  - `partition_fields` (list[string] | null): Partition key fields for export.
  - `geometry_column` (string | null): Geometry column for import.
  - `batch_size` (int): Record batch size.
  - `overwrite` (bool): Replace existing output.
  - `include_centroids` (bool): Add centroid geometry column on export.
  - `name` (string | null): Optional dataset part-name prefix.
- Validation rules:
  - `batch_size` must be > 0.
  - `partition_fields` values must exist in selected attributes.
  - `geometry_column` must exist in GeoParquet geometry metadata on import.

## Entity: GeoParquetDataset

- Purpose: Represents persisted GeoParquet output/input with required metadata.
- Fields:
  - `path` (string): Dataset root or single file path.
  - `layout` (enum): `single_file`, `dataset`, `hive_partitioned`.
  - `part_file_pattern` (string): `part-<name?>_<runid>-<i>.parquet` pattern.
  - `geo_metadata` (object): GeoParquet metadata payload (`version`, `primary_column`, `columns`).
  - `columns` (list[ColumnDescriptor]): Logical schema columns.
- Validation rules:
  - Must include `geo` metadata with `version` and `primary_column`.
  - Primary geometry column must use `WKB` encoding.

## Entity: GeometryColumnDescriptor

- Purpose: Defines geometry column semantics from GeoParquet metadata.
- Fields:
  - `name` (string): Column name.
  - `encoding` (enum): Expected `WKB`.
  - `geometry_types` (list[string]): Allowed geometry types.
  - `crs` (object | null): CRS object, typically PROJJSON-derived.
  - `is_primary` (bool): Whether this column is primary for feature class creation.
- Validation rules:
  - `encoding` must be `WKB`.
  - `geometry_types` must be non-empty for strict validation.

## Entity: ToolParameterMapping

- Purpose: Maps ArcGIS toolbox parameters to Python API args.
- Fields:
  - `tool_name` (string): Toolbox tool class name.
  - `parameter_name` (string): UI parameter key.
  - `api_argument` (string): Target API argument.
  - `required` (bool): Requirement status.
  - `default_value` (any): Default when omitted.
- Validation rules:
  - Required parameters must map to non-null API arguments.
  - Added parameters (`batch_size`, `geometry_column`, `include_centroids`, `name`) must have documented defaults.

## Relationships

- A `ConversionJob` produces or consumes one `GeoParquetDataset`.
- A `GeoParquetDataset` has one or more `GeometryColumnDescriptor` entries.
- A toolbox execution uses multiple `ToolParameterMapping` rows to build one `ConversionJob`.

## State Transitions

- ConversionJob lifecycle:
  - `draft` -> `validated` -> `running` -> (`completed` | `failed`).
- GeoParquetDataset lifecycle (export path):
  - `not_created` -> `writing_parts` -> `metadata_finalized` -> `available`.
- Compatibility lifecycle:
  - `legacy_api_active` -> `legacy_api_warned` -> `legacy_api_removed` (future major release).
