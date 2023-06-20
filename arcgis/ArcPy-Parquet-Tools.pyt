# -*- coding: utf-8 -*-
import importlib
import json
from pathlib import Path
from typing import List
import sys

import arcpy
import pyarrow.parquet as pq

# import the local library, either if in system environment, or through relative path
if importlib.util.find_spec("arcpy_parquet") is None:
    dir_src = Path(__file__).parent.parent/'src'
    pkg_dir_lst = [pth for pth in dir_src.glob('*') if pth.stem == 'arcpy_parquet']
    if len(pkg_dir_lst) == 0:
        raise Exception('To use ArcPy-Parquet-Tools, you need to either run "make env" or keep the toolbox with '
                        'the project.')
    else:
        sys.path.insert(0, str(dir_src))

from arcpy_parquet import (
    create_schema_file,
    parquet_to_feature_class,
    feature_class_to_parquet
)


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the .pyt file)."""
        self.label = "ArcPy-Parquet-Tools"
        self.alias = "arcpyParquetTools"

        # List of tool classes associated with this toolbox
        self.tools = [CreateSchemaFile, ParquetToFeatureClass, FeatureClassToParquet]


class FeatureClassToParquet(object):

    def __init__(self):
        self.label = "Feature Class to Parquet"
        self.description = "Convert a Feature Class to properly formatted Parquet for use with Spark."

    def getParameterInfo(self):

        input_table = arcpy.Parameter(
            name='input_table',
            displayName='Input Feature Class',
            direction='Input',
            datatype='GPFeatureLayer',
            parameterType='Required'
        )

        output_parquet = arcpy.Parameter(
            name="pqt_pth",
            displayName="Output Parquet",
            direction="Output",
            datatype="DEFolder",
            parameterType="Required"
        )

        geometry_format = arcpy.Parameter(
            name="geometry_format",
            displayName="Geometry Format",
            direction="Input",
            datatype="GPString",
            category='Advanced',
            parameterType="Required"
        )
        geometry_format.filter.type = "ValueList"
        geometry_format.filter.list = ["WKB", "WKT", "XY"]
        geometry_format.value = "WKB"

        param_lst = [input_table, output_parquet, geometry_format]

        return param_lst

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):

        # since input can also be layer, get path to  the input data
        in_tbl = parameters[0]
        in_tbl_pth = Path(arcpy.Describe(in_tbl).catalogPath)

        # provide output as actual path object
        out_pqt_pth = Path(parameters[1].valueAsText)

        # get the literal value for the output format
        geometry_format = parameters[2].valueAsText

        feature_class_to_parquet(
            input_table=in_tbl_pth,
            output_parquet=out_pqt_pth,
            include_geometry=True,
            geometry_format=geometry_format,
            batch_size=300000
        )

        return


class ParquetToFeatureClass(object):

    def __init__(self):
        self.label = "Parquet to Feature Class"
        self.description = ("Convert a Parquet file with geometries saved as well known binary (WKB) to a Feature "
                            "Class in a GeoDatabase.")

    def get_partition_lst(self, dir_pth: Path) -> List[str]:
        """
        Helper function to get the directories matching the partitioning pattern.
        Args:
            dir_pth: Path to directory.

        Returns:
            List of directory names for child partitions.
        """
        return [p.stem for p in dir_pth.glob('*') if p.is_dir() and p.stem]

    def getParameterInfo(self):
        pqt_pth = arcpy.Parameter(
            name="pqt_pth",
            displayName="Parquet Folder",
            direction="Input",
            datatype="DEFolder",
            parameterType="Required"
        )

        pqt_prt_01 = arcpy.Parameter(
            name='pqt_prt_01',
            displayName='Parquet Partition',
            direction='Input',
            datatype='GPString',
            parameterType='Optional',
            enabled=False
        )

        pqt_prt_02 = arcpy.Parameter(
            name='pqt_prt_02',
            displayName='Parquet Partition',
            direction='Input',
            datatype='GPString',
            parameterType='Optional',
            enabled=False
        )

        pqt_prt_03 = arcpy.Parameter(
            name='pqt_prt_03',
            displayName='Parquet Partition',
            direction='Input',
            datatype='GPString',
            parameterType='Optional',
            enabled=False
        )

        sptl_ref = arcpy.Parameter(
            name='sptl_ref',
            displayName='Spatial Reference',
            direction='Input',
            datatype='GPSpatialReference',
            parameterType='Required'
        )
        sptl_ref.value = arcpy.SpatialReference(4326).exportToString()

        geom_type = arcpy.Parameter(
            name="geom_type",
            displayName="Geometry Type",
            direction="Input",
            datatype="GPString",
            parameterType="Required"
        )
        geom_type.filter.type = "ValueList"
        geom_type.filter.list = ["POINT", "POLYLINE", "POLYGON"]
        geom_type.value = "POINT"

        out_fc_pth = arcpy.Parameter(
            name='out_fc_pth',
            displayName='Output Feature Class Path',
            datatype='DEFeatureClass',
            direction='Output',
            parameterType='Required'
        )

        geom_col = arcpy.Parameter(
            name='geom_col',
            displayName='Geometry Column',
            datatype='GPString',
            direction='Input',
            category='Advanced',
            parameterType='Required'
        )
        geom_col.filter.type = 'ValueList'
        geom_col.value = 'wkb'

        build_idx = arcpy.Parameter(
            name='build_idx',
            displayName='Build Spatial Index',
            datatype='GPBoolean',
            direction='Output',
            category='Advanced',
            parameterType='Required'
        )
        build_idx.value = True

        smpl = arcpy.Parameter(
            name='smpl',
            displayName="Export Sample",
            datatype='GPBoolean',
            direction='Input',
            category='Advanced',
            parameterType='Required'
        )
        smpl.value = False

        smpl_cnt = arcpy.Parameter(
            name='smpl_cnt',
            displayName='Sample Count',
            datatype='GPLong',
            direction='Input',
            category='Advanced',
            parameterType='Required',
            enabled=False
        )
        smpl_cnt.value = 100

        schema_file_pth = arcpy.Parameter(
            name='schema_file_pth',
            displayName='Schema File',
            datatype='DEFile',
            direction='Input',
            category='Advanced',
            parameterType='Optional'
        )
        schema_file_pth.filter.list = ['csv']

        param_lst = [pqt_pth, pqt_prt_01, pqt_prt_02, pqt_prt_03, sptl_ref, out_fc_pth, geom_col, geom_type, build_idx,
                     smpl, smpl_cnt, schema_file_pth]

        return param_lst

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # unpack the parameters into understandable variable names
        pqt_pth, pqt_prt_01, pqt_prt_02, pqt_prt_03, sptl_ref, out_fc_pth, geom_col, geom_type, build_idx, smpl, smpl_cnt, schema_file_pth = parameters

        # pre-populate the output name
        if pqt_pth.altered and out_fc_pth.value is None:
            fc_name = out_fc_pth.stem
            out_fc_pth.value = str(Path(arcpy.env.currentWorkspace) / fc_name)

        # take actions if parquet path is provided
        if pqt_pth.altered:

            # get a path object
            p_pth = Path(pqt_pth.valueAsText)

            # ensure somewhere under the top level directories, the actual parquet part files are found
            part_lst = [itm for itm in p_pth.rglob('part-*')]
            if len(part_lst) == 0:
                pqt_pth.setErrorMessage('Cannot locate parquet "part" files.')

            # get a list of any level one partitions
            prt_01_lst = self.get_partition_lst(p_pth)

            # update the parquet part parameter if there are partition directories
            if len(prt_01_lst):
                pqt_prt_01.filter.type = 'ValueList'
                pqt_prt_01.filter.list = prt_01_lst
                pqt_prt_01.enabled = True

        # if the partition 01 is modified, populate and enable level two if applicable
        if pqt_prt_01.altered:
            prt_02_lst = self.get_partition_lst(p_pth/pqt_prt_01.value)
            if len(prt_02_lst):
                pqt_prt_02.filter.type = 'ValueList'
                pqt_prt_02.filter.list = prt_02_lst
                pqt_prt_02.enabled = True

        # if the partition 02 is modified, populate and enable level three if applicable
        if pqt_prt_02.altered:
            prt_03_lst = self.get_partition_lst(p_pth/pqt_prt_01.value/pqt_prt_02.value)
            if len(prt_03_lst):
                pqt_prt_03.filter.type = 'ValueList'
                pqt_prt_03.filter.list = prt_03_lst
                pqt_prt_03.enabled = True

        # if a sample is desired, add ability to specify the sample count
        if smpl.value is True and smpl_cnt.value < 1:
            smpl_cnt.setErrorMessage('Sample count must be greater than zero.')
        elif smpl.altered and smpl.value is True:
            smpl_cnt.enabled = True
        elif smpl.altered and smpl.value is False and smpl_cnt.value < 1:
            smpl_cnt.value = 100
        elif smpl.altered and smpl.value is False:
            smpl_cnt.enabled = False

        # if an input parquet dataset is provided, read the columns to provide options for the advanced input
        if pqt_pth.altered:

            pqt_ds = pq.ParquetDataset(pqt_pth.valueAsText, use_legacy_dataset=False)

            col_lst = pqt_ds.schema.names
            geom_col.filter.list = col_lst

            # set a default if wkb in input column name string
            for col in col_lst:
                if 'wkb' in col.lower():
                    geom_col.value = col
                    break

        # if following convention with schema in a nearby directory, when parquet path is provided, search for schema
        if pqt_pth.altered:

            # get the common parent directory for the parquet dataset
            ds_pth = Path(pqt_pth.valueAsText).parent

            # look to see if the schema has been included
            schm_lst = list(ds_pth.glob('**/schema'))
            if len(schm_lst):
                schm_dir_pth = schm_lst[0]

                # pull out the path file if it can be found
                csv_pth_lst = list(schm_dir_pth.glob('**/part*.csv'))
                if len(csv_pth_lst):
                    csv_pth = csv_pth_lst[0]

                    # populate the schema file path so don't have to hunt for it if using the convention
                    schema_file_pth.value = str(csv_pth)

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # unpack the parameters
        pqt_pth = parameters[0].valueAsText
        pqt_prt_01 = parameters[1].valueAsText
        pqt_prt_02 = parameters[2].valueAsText
        pqt_prt_03 = parameters[3].valueAsText
        sptl_ref = parameters[4].value
        out_fc_pth = parameters[5].valueAsText
        geom_col = parameters[6].valueAsText
        geom_type = parameters[7].valueAsText
        build_idx = parameters[8].value
        smpl = parameters[9].value
        smpl_cnt = parameters[10].valueAsText
        schema_file_pth = parameters[11].valueAsText

        # pass the JSON path in as a Path object
        if schema_file_pth is not None:
            schema_file_pth = Path(schema_file_pth)

        # partitions
        partition_vals = [pqt_prt_01, pqt_prt_02, pqt_prt_03]
        partition_vals = [p for p in partition_vals if p is not None]  # not none

        # optionally add and format outputting a sample
        if smpl is True:
            smpl_cnt = int(smpl_cnt)
        else:
            smpl_cnt = None

        # execute the function
        parquet_to_feature_class(pqt_pth, output_feature_class=Path(out_fc_pth), schema_file=schema_file_pth,
                                 geometry_type=geom_type, parquet_partitions=partition_vals,
                                 geometry_column=geom_col, spatial_reference=sptl_ref, sample_count=smpl_cnt,
                                 build_spatial_index=build_idx)

        return


class CreateSchemaFile(object):

    def __init__(self):
        self.label = "Create Schema File"
        self.description = "Create a CSV schema file from an existing Feature Class to use with imports."
        self.category = "Utilities"

    def getParameterInfo(self):

        tmplt_fc_pth = arcpy.Parameter(
            name='tmplt_fc_pth',
            displayName='Template Feature Class Path',
            datatype='DEFeatureClass',
            direction='Input',
            parameterType='Required'
        )

        schema_file_pth = arcpy.Parameter(
            name='schema_file_pth',
            displayName='Schema File',
            datatype='DEFile',
            direction='Output',
            parameterType='Optional'
        )

        param_lst = [tmplt_fc_pth, schema_file_pth]

        return param_lst

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # unpack the parameters
        tmplt_fc_pth = Path(parameters[0].valueAsText)
        schema_file_pth = Path(parameters[1].valueAsText)

        # execute the function
        create_schema_file(template_feature_class_path=tmplt_fc_pth, output_schema_file=schema_file_pth)

        return
