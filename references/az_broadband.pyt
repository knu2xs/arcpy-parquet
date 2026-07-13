# -*- coding: utf-8 -*-
__version__ = "2.0.0.dev0"
__author__ = ""
__license__ = "Apache 2.0"

import importlib.util
from pathlib import Path
import sys

import arcpy


def find_pkg_source(package_name) -> Path:
    """Helper to find relative package name"""
    # get the path to the current directory
    file_dir = Path(__file__).parent

    # try to find the package in progressively higher levels
    for idx in range(4):
        tmp_pth = file_dir / "src" / package_name
        if tmp_pth.exists():
            return tmp_pth.parent
        else:
            file_dir = file_dir.parent

    # if nothing fund, nothing returned
    return None


# account for using relative path to package
if importlib.util.find_spec("az_broadband") is None:
    src_dir = find_pkg_source("az_broadband")
    if src_dir is not None:
        sys.path.append(str(src_dir))

# include custom code
import az_broadband
from az_broadband.utils import get_logger


class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the .pyt file)."""
        self.label = "az-broadband"
        self.alias = "az_broadband"

        # List of tool classes associated with this toolbox
        self.tools = [
            FeaturesToGeoParquet,
            GeoParquetToFeatures,
        ]


class FeaturesToGeoParquet:
    """Convert a feature layer to a partitioned GeoParquet dataset."""

    # Maximum number of cascading partition-by field parameters
    _MAX_PARTITION_FIELDS = 5

    def __init__(self):
        self.label = "Features To GeoParquet"
        self.description = (
            "Export a feature layer to a GeoParquet dataset in a folder, "
            "with optional Hive-style partitioning by one or more fields."
        )
        self.category = "GeoParquet"

        # configure logging
        logger_name = f"az_broadband.Toolbox.{self.__class__.__name__}"
        self.logger = get_logger(logger_name, level="INFO", add_arcpy_handler=True)

    def getParameterInfo(self):
        """Define parameter definitions."""

        input_features = arcpy.Parameter(
            displayName="Input Features",
            name="input_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input",
        )

        output_folder = arcpy.Parameter(
            displayName="Output Folder",
            name="output_folder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input",
        )

        overwrite = arcpy.Parameter(
            displayName="Overwrite Existing Output",
            name="overwrite",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        overwrite.value = False

        # Build cascading partition-by field parameters.
        # Each subsequent parameter only becomes enabled when the previous
        # one has a value, so the user can select 0 to _MAX_PARTITION_FIELDS
        # partitioning fields in order.
        partition_params = []
        for i in range(1, self._MAX_PARTITION_FIELDS + 1):
            ordinal = self._ordinal(i)
            p = arcpy.Parameter(
                displayName=f"Partition Field ({ordinal})",
                name=f"partition_field_{i}",
                datatype="GPString",
                parameterType="Optional",
                direction="Input",
            )
            p.filter.type = "ValueList"
            # Only the first partition field is initially enabled
            if i > 1:
                p.enabled = False
            partition_params.append(p)

        params = [input_features, output_folder, overwrite] + partition_params
        return params

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.

        Dynamically populates each partition-field drop-down with the
        attribute fields of the input features, excluding any fields
        already selected in earlier partition parameters.  A new
        partition parameter is enabled only when the previous one has a
        value.
        """
        input_features = parameters[0]

        # Indices of the partition-field parameters
        part_start = 3
        part_end = part_start + self._MAX_PARTITION_FIELDS

        if not input_features.value:
            # No input: clear and disable all partition parameters
            for idx in range(part_start, part_end):
                parameters[idx].filter.list = []
                parameters[idx].value = None
                parameters[idx].enabled = idx == part_start
            return

        # Get the list of non-OID, non-Geometry field names
        if input_features.altered:
            desc = arcpy.Describe(input_features.valueAsText)
            all_field_names = [
                f.name
                for f in desc.fields
                if f.type not in ("OID", "Geometry") and f.name != desc.shapeFieldName
            ]
        else:
            all_field_names = []

        # Track which fields have already been selected
        selected: list[str] = []

        for idx in range(part_start, part_end):
            param = parameters[idx]
            is_first = idx == part_start
            prev_param = parameters[idx - 1] if not is_first else None

            # Enable this parameter only if the previous one has a value
            # (the first partition param is always enabled when features exist)
            if is_first:
                param.enabled = True
            else:
                param.enabled = bool(prev_param and prev_param.value)

            if param.enabled:
                # Offer only fields not already chosen by earlier params
                available = [f for f in all_field_names if f not in selected]
                param.filter.list = available

                # If the current value was cleared or is no longer valid,
                # also clear all subsequent parameters
                if param.value and param.valueAsText not in available:
                    param.value = None

                if param.value:
                    selected.append(param.valueAsText)
            else:
                param.filter.list = []
                param.value = None

    def execute(self, parameters, messages):
        """The source code of the tool."""
        from az_broadband.utils.parquet import features_to_geoparquet

        input_features = parameters[0].valueAsText
        output_folder = parameters[1].valueAsText
        overwrite = parameters[2].value or False

        # Collect selected partition fields in order
        part_start = 3
        partition_fields = []
        for idx in range(part_start, part_start + self._MAX_PARTITION_FIELDS):
            val = parameters[idx].valueAsText
            if val:
                partition_fields.append(val)
            else:
                break

        self.logger.info(f"Input Features: {input_features}")
        self.logger.info(f"Output Folder:  {output_folder}")
        if partition_fields:
            self.logger.info(f"Partition By:   {', '.join(partition_fields)}")
        else:
            self.logger.info("Partition By:   (none)")

        result = features_to_geoparquet(
            feature_class=input_features,
            output_path=output_folder,
            partition_fields=partition_fields if partition_fields else None,
            overwrite=overwrite,
        )

        self.logger.info(f"GeoParquet dataset written to: {result}")

        return

    @staticmethod
    def _ordinal(n: int) -> str:
        """Return the ordinal string for an integer (1st, 2nd, 3rd, …)."""
        if 11 <= (n % 100) <= 13:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"


class GeoParquetToFeatures:
    """Import a GeoParquet dataset into an Esri Feature Class."""

    def __init__(self):
        self.label = "GeoParquet To Features"
        self.description = (
            "Import a GeoParquet file or dataset directory into a Feature Class, "
            "optionally selecting the geometry column to use."
        )
        self.category = "GeoParquet"

        # configure logging
        logger_name = f"az_broadband.Toolbox.{self.__class__.__name__}"
        self.logger = get_logger(logger_name, level="INFO", add_arcpy_handler=True)

    def getParameterInfo(self):
        """Define parameter definitions."""

        input_parquet = arcpy.Parameter(
            displayName="Input GeoParquet",
            name="input_parquet",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input",
        )

        output_feature_class = arcpy.Parameter(
            displayName="Output Feature Class",
            name="output_feature_class",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output",
        )

        geometry_column = arcpy.Parameter(
            displayName="Geometry Column",
            name="geometry_column",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        geometry_column.filter.type = "ValueList"

        overwrite = arcpy.Parameter(
            displayName="Overwrite Existing Output",
            name="overwrite",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        overwrite.value = False

        params = [input_parquet, output_feature_class, geometry_column, overwrite]
        return params

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.

        Dynamically populates the geometry column drop-down with the
        geometry columns declared in the GeoParquet metadata.
        """
        input_parquet = parameters[0]
        geometry_column = parameters[2]

        if input_parquet.altered and input_parquet.value:
            try:
                from az_broadband.utils.parquet import get_geometry_columns

                geom_cols = get_geometry_columns(input_parquet.valueAsText)
                geometry_column.filter.list = geom_cols
            except Exception:
                geometry_column.filter.list = []
        elif not input_parquet.value:
            geometry_column.filter.list = []

    def execute(self, parameters, messages):
        """The source code of the tool."""
        from az_broadband.utils.parquet import geoparquet_to_features

        input_parquet = parameters[0].valueAsText
        output_feature_class = parameters[1].valueAsText
        geometry_column = parameters[2].valueAsText or None
        overwrite = parameters[3].value or False

        self.logger.info(f"Input GeoParquet:      {input_parquet}")
        self.logger.info(f"Output Feature Class:  {output_feature_class}")
        self.logger.info(f"Geometry Column:       {geometry_column or '(primary)'}")

        result = geoparquet_to_features(
            parquet_path=input_parquet,
            feature_class=output_feature_class,
            geometry_column=geometry_column,
            overwrite=overwrite,
        )

        self.logger.info(f"Feature class created: {result}")

        return


