# Data Model: Efficient GeoParquet Integration

## Entity: ConversionJob

- Purpose: Runtime configuration for one export or import operation.
- Fields:
  - `job_id` (string): Trace identifier.
  - `operation` (enum): `export` or `import`.
  - `source_path` (string): Input feature class or parquet dataset path.
  - `target_path` (string): Output dataset or feature class path.
  - `fields` (list[string] | null): Selected non-geometry fields.
  - `partition_fields` (list[string] | null): Export partition keys.
  - `geometry_column` (string | null): Selected geometry column for import.
  - `batch_size` (int): Processing batch size.
  - `overwrite` (bool): Output replacement flag.
  - `include_centroids` (bool): Optional centroid column output.
  - `name` (string | null): Optional part file prefix.
- Validation rules:
  - `batch_size` must be > 0.
  - `partition_fields` must be a subset of available/exported fields.
  - `geometry_column` must be present in declared GeoParquet geometry metadata when provided.

## Entity: GeoParquetDataset

- Purpose: File or directory-based dataset conforming to GeoParquet metadata expectations.
- Fields:
  - `path` (string): Single-file or dataset root path.
  - `layout` (enum): `single_file`, `dataset`, `hive_partitioned`.
  - `geo_metadata` (object): `geo` metadata block (`version`, `primary_column`, `columns`).
  - `partitions` (list[PartitionDescriptor] | null): Optional partition descriptors.
  - `columns` (list[ColumnDescriptor]): Physical/logical schema descriptors.
- Validation rules:
  - Must include required `geo` metadata keys for conforming output.
  - Primary geometry column must be WKB-encoded.

## Entity: GeometryColumnDescriptor

- Purpose: Logical description of geometry columns for import/export behavior.
- Fields:
  - `name` (string): Column name.
  - `encoding` (enum): Expected `WKB`.
  - `geometry_types` (list[string]): Declared geometry type set.
  - `crs` (object | null): CRS payload from metadata.
  - `is_primary` (bool): Primary geometry selector.
- Validation rules:
  - `encoding` must equal `WKB`.
  - `name` must map to an existing schema column.

## Entity: ToolParameterMapping

- Purpose: Translation contract from ArcGIS toolbox parameters to Python API arguments.
- Fields:
  - `tool_name` (string): Toolbox class/tool identity.
  - `parameter_name` (string): ArcGIS parameter key.
  - `api_argument` (string): Python function argument name.
  - `required` (bool): Requirement status.
  - `default_value` (any): Applied default when omitted.
- Validation rules:
  - Required toolbox parameters must map to non-null required API arguments.
  - Added optional parameters (`batch_size`, `geometry_column`, `include_centroids`, `name`) must have deterministic defaults.

## Relationships

- One `ConversionJob` reads from or writes to one `GeoParquetDataset`.
- One `GeoParquetDataset` has one or more `GeometryColumnDescriptor` entries.
- A toolbox execution resolves a set of `ToolParameterMapping` entries into one `ConversionJob`.

## State Transitions

- `ConversionJob`: `draft` -> `validated` -> `running` -> (`completed` | `failed`).
- `GeoParquetDataset` (export): `not_created` -> `writing` -> `metadata_finalized` -> `available`.
- Compatibility lifecycle: `legacy_active` -> `legacy_warned` -> `legacy_removed` (future major release).
