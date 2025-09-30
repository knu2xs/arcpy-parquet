---
title: Home
---
# ArcPy Parquet 0.2.0.dev0 Documentation

ArcPy Parquet is provides the ability to convert data between local ArcGIS Pro Feature Classes and Parquet datasets,
including Geoparquet. While ArcGIS Pro does include the ability to work with Parquet data natively, it can only work 
with single file parquet files. Since Parquet data frequently is partitioned and sharded, this project enables working 
with partitioned and sharded Parquet datasets. It also is designed to handle extremely large datasets.

ArcPy Parquet is comprised of an installable Python package located in `./src` along with an ArcGIS Python Toolbox, 
which can be used in ArcGIS Pro.

If you download the entire ArcPy Parquet repository to your local machine, and keep all the downloaded assets 
together in the same location, you do not need to customize your Python configuration at all. The toolbox knows 
how to find the Python package relative to itself, and will include all functionality.