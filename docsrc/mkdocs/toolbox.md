---
title: ArcGIS Pro Toolbox
---
# ArcGIS Pro Toolbox

Most ArcGIS Pro users should start here.

The ArcPy Parquet toolbox file ArcPy-Parquet-Tools.pyt is designed to make the package features available in ArcGIS Pro geoprocessing workflows without writing Python code.

## What This Is

The toolbox is included in this repository under arcgis and can be opened directly in ArcGIS Pro.

Primary tools:

- FeatureClassToParquet: Export ArcGIS feature classes to Parquet with GeoParquet as the default geometry standard.
- GeoparquetToFeatureClass: Import Parquet or GeoParquet datasets into ArcGIS feature classes.
- CreateSchemaFile: Generate CSV schema templates to support consistent data conversion workflows.

## How It Maps To The Python Package

Tool execution is aligned to the same public Python functions used by developers:

- FeatureClassToParquet maps to features_to_parquet.
- GeoparquetToFeatureClass maps to parquet_to_features.
- CreateSchemaFile maps to create_schema_file.

This means ArcGIS Pro and Python users share the same conversion behavior.

## Packaging For Distribution

If you need to distribute the toolbox to non-technical users, use the packaging workflow in scripts/make_pyt_archive.py (invoked by make pyt_pkg). This bundles:

- ArcPy-Parquet-Tools.pyt
- src/arcpy_parquet
- Package dependencies declared by the project

Keep toolbox XML docs and package versioning aligned so parameter behavior is consistent across releases.

## Next Step

- If you are using ArcGIS Pro tools: continue with this toolbox page and the toolbox XML help in arcgis.
- If you are developing in Python: go to the Python API page at api.md.
