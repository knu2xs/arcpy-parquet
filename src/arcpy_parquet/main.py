import logging
from pathlib import Path
import time
from typing import List, Optional, Tuple, Union, Iterable
import uuid

import arcpy
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .arcpy_logging import get_logger

__all__ = ["create_schema_file", "parquet_to_feature_class", "feature_class_to_parquet"]

geom_dict = {
    "COORDINATES": ("POINT", "DISABLED", "DISABLED"),
    "POINT": ("POINT", "DISABLED", "DISABLED"),
    "POINT M": ("POINT", "ENABLED", "DISABLED"),
    "POINT Z": ("POINT", "DISABLED", "ENABLED"),
    "POLYLINE": ("POLYLINE", "DISABLED", "DISABLED"),
    "POLYLINE M": ("POLYLINE", "ENABLED", "DISABLED"),
    "POLYLINE Z": ("POLYLINE", "DISABLED", "ENABLED"),
    "POLYGON": ("POLYGON", "DISABLED", "DISABLED"),
    "POLYGON M": ("POLYGON", "ENABLED", "DISABLED"),
    "POLYGON Z": ("POLYGON", "DISABLED", "ENABLED"),
    "MULTIPOINT": ("MULTIPOINT", "DISABLED", "DISABLED"),
    "MULTIPOINT M": ("MULTIPOINT", "ENABLED", "DISABLED"),
    "MULTIPOINT Z": ("MULTIPOINT", "DISABLED", "ENABLED"),
}
import_dtype_dict = {
    "int8": "INTEGER",
    "int16": "INTEGER",
    "int32": "INTEGER",
    "int64": "LONG",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "times": "DATE",
    "decim": "DOUBLE",
    "decimal": "DOUBLE",
    "string": "TEXT",
    "date": "DATE",
    "date32[day]": "DATE",
}
export_dtype_dict = {
    "OID": pa.int64(),
    "Date": pa.timestamp("s"),
    "Double": pa.float64(),
    "Integer": pa.int64(),
    "Single": pa.float32(),
    "SmallInteger": pa.int16(),
    "Blob": pa.large_binary(),
    "String": pa.string(),
}


def feature_class_to_parquet(
    input_table: Path,
    output_parquet: Path,
    include_geometry: bool = True,
    geometry_format: str = "WKB",
    batch_size: int = 300000,
) -> Path:
    """
    Export a Feature Class to Parquet.

    Args:
        input_table: Path to feature class or table.
        output_parquet: Path to where the output Parquet file will be saved.
        include_geometry: Whether to include the geometry in the output Parquet dataset.
        geometry_format: If including the geometry, what format the geometry should be in, either
            ``XY``, ``WKT``, or ``WKB``. Default is ``WKB``.
        batch_size: Count of records per ``part-*.parquet`` file.
    """
    # downgrade path so arcpy can use strings
    fc_pth = str(input_table)

    # check the geometry output type
    geometry_format = geometry_format.upper()
    assert geometry_format in (
        "XY",
        "WKT",
        "WKB",
    ), "Geometry output format must be either XY, WKB or WKT."

    # make sure the full output path exists where the data_dir will be saved
    if not output_parquet.exists():
        output_parquet.mkdir(parents=True)

    # describe the data to access properties for validation
    desc = arcpy.da.Describe(fc_pth)

    # fields to be excluded
    exclude_fld_typ = ["Raster"]
    exclude_fld_lst = []

    if desc["hasOID"]:
        exclude_fld_lst.append(desc["OIDFieldName"])

    for shp_fld_key in ["lengthFieldName", "areaFieldName"]:

        shp_fld = desc.get(shp_fld_key)
        if shp_fld is not None:
            exclude_fld_lst.append(shp_fld)

    # get a list of input fields to use with the search cursor
    sc_col_lst = [
        f.name
        for f in desc["fields"]
        if f.name not in exclude_fld_lst and f.type not in exclude_fld_typ
    ]

    # iterate fields to create output schema, excluding geometry since handled explicitly later
    pa_fld_typ_xcld = exclude_fld_typ + ["Geometry"]
    pa_fld_lst = [
        f
        for f in desc["fields"]
        if f.name not in exclude_fld_lst and f.type not in pa_fld_typ_xcld
    ]
    pa_typ_lst = [
        (f.name, export_dtype_dict.get(f.type, pa.string())) for f in pa_fld_lst
    ]
    pq_schema = pa.schema(pa_typ_lst)

    # if the input has geometry (is a feature class)
    if desc.get("shapeFieldName") is not None and include_geometry:

        # get the name of the geometry column
        geom_nm = desc.get("shapeFieldName")

        # since the geometry must be specially formatted, it needs to first be removed from the list
        sc_col_lst.remove(geom_nm)

        # if just outputting centroid (point coordinates)
        if pq_schema == "XY":

            # add X and Y columns to search cursor list and the output schema
            for prt in ["X", "Y"]:
                sc_col_lst.append(f"{geom_nm}@{prt}")
                pq_schema = pq_schema.append(pa.field(f"geometry_{prt}", pa.float64()))

        # if working with any other output type, just use the specific output format
        else:

            # add the correct search cursor type
            sc_col_lst.append(f"{geom_nm}@{geometry_format}")

            # add the geometry to the output schema
            pq_schema = pq_schema.append(
                pa.field(f"{geometry_format.lower()}", pa.binary())
            )

    # if the geometry is not desired in the output, remove it from the search cursor column list
    if "shapeFieldName" in desc.keys() and not include_geometry:
        sc_col_lst.remove(desc["shapeFieldName"])

    # get values from the data to track progress
    max_range = int(arcpy.management.GetCount(str(input_table))[0])
    rep_range = max(1, max_range // 100)

    # report progress
    features_tense = "feature" if max_range == 1 else "features"
    arcpy.AddMessage(f"Starting export of {max_range:,} {features_tense}.")
    arcpy.SetProgressor("step", "Exporting...", 0, max_range, rep_range)

    # turn off auto cancelling since handling in loop
    arcpy.env.autoCancelling = False

    # create a template dictionary for data_dir export
    pa_dict = {col: [] for col in pq_schema.names}

    # create a search cursor to work through the data_dir
    with arcpy.da.SearchCursor(str(input_table), sc_col_lst) as search_cur:

        # variable for batch numbering
        prt_num = 0

        # begin to iterate through the features
        for idx, row in enumerate(search_cur):

            # add each row column partition_column_list to the respective key in the dictionary
            for col, val in zip(pq_schema.names, row):
                pa_dict[col].append(val)

            # if at a percent interval
            if idx % rep_range == 0:

                # report progress
                arcpy.SetProgressorPosition(idx)

                # check if cancelled, and if so, break out
                if arcpy.env.isCancelled:
                    break

            # if at a batch size (part) interval or end of dataset
            if (idx + 1) % batch_size == 0 or (idx + 1) == max_range:
                # create a PyArrow table object instance from the accumulated dictionary
                pa_tbl = pa.Table.from_pydict(pa_dict, pq_schema)

                # create a name and path string for the table
                part_nm = f"part-{uuid.uuid4().hex}-{prt_num:05d}.snappy.parquet"
                part_pth = output_parquet / part_nm

                # write the parquet part
                pq.write_table(
                    table=pa_tbl,
                    where=str(part_pth),
                    # filesystem=pa.filesystem.LocalFileSystem,
                    version="2.0",
                    flavor="spark_session",
                    compression="snappy",
                )

                # increment the naming index
                prt_num += 1

                # reset the loading dictionary
                pa_dict = {col: [] for col in pq_schema.names}

    # reset the progress indicator
    arcpy.ResetProgressor()

    return output_parquet


def get_partition_lst(dir_pth: Path) -> List[str]:
    """
    Helper function to get the directories matching the partitioning pattern.
    Args:
        dir_pth: Path to directory.

    Returns:
        List of directory names for child partitions.
    """
    return [p.stem for p in dir_pth.glob("*=*") if p.is_dir() and p.stem]


def get_partition_column(pqt_prt: str) -> str:
    """
    Helper function to get parquet partition column names from directory path string
    separated by equals(``=``).

    Args:
        pqt_prt: Parquet partition directory string name.

    Returns:
        Column name and partition_column_list to use.
    """
    col_nm = tuple(pqt_prt.split("="))[0]
    return col_nm


def get_available_partitions(pqt_pth: Path) -> Tuple[str]:
    """
    Helper function to get a list of partition columns used.

    Args:
        pqt_pth: Path to parquet data_dir.

    Returns:
        Tuple of any partitioned columns.
    """
    pqt_pth = pqt_pth if isinstance(pqt_pth, Path) else Path(pqt_pth)
    pqt_cols = set(p for p in pqt_pth.glob("**/*=*") if p.is_dir())

    return pqt_cols


def parquet_to_feature_class(
    parquet_path: Path,
    output_feature_class: Path,
    schema_file: Path = None,
    geometry_type: str = "POINT",
    parquet_partitions: Optional[List[str]] = None,
    geometry_column: Union[List[str], str] = "wkb",
    spatial_reference: Union[arcpy.SpatialReference, str, int] = 4326,
    sample_count: Optional[int] = None,
    build_spatial_index: bool = False,
    compact: bool = True,
) -> Path:
    """
    Convert a *properly formatted* Parquet source into a Feature Class in a Geodatabase.

    .. note::
        The geometry *must* be encoded as well known binary (WKB), and complex field types
        such as arrays and structs are not supported.

    Args:
        parquet_path: The directory or Parquet part file to convert.
        output_feature_class: Where to save the new Feature Class.
        schema_file: CSV file with detailed schema properties.
        geometry_type: ``POINT``, ``COORDINATES``, ``POLYLINE``, or ``POLYGON`` describing the geometry type. Default
            is ``POINT``. ``COORDINATES``, is a point geometry type described by two coordinate columns.
        parquet_partitions: Partition name and values, if available, to select. For instance,
            if partitioned by country column using ISO2 identifiers, select Mexico using
            ``country=mx``.
        geometry_column: Column from parquet table containing the geometry encoded as WKB. Default
            is ``wkb``. If the geometry type is ``COORDINATES``, this must be an iterable (tuple or list) of
            the x (longitude) and y (latitude) columns containing the coordinates.
        spatial_reference: Spatial reference of input data. Default is WGS84 (WKID: 4326).
        sample_count: If only wanting to import enough data to understand the schema, specify
            the count of records with this parameter. If left blank, will import all records.
        build_spatial_index: Optional if desired to build the spatial index once inserting all the data.
            Default is ``False``.
        compact: If the File Geodatabase should be compacted following import. Default is ``True``.
    """
    # convert integer to Spatial Reference
    if isinstance(spatial_reference, (int, str)):
        spatial_reference = arcpy.SpatialReference(spatial_reference)

    # if used as a tool in pro, let user know what is going on...kind of
    arcpy.SetProgressorLabel("Warming up the Flux Capacitor...")

    # make sure paths...are paths
    parquet_path = (
        parquet_path if isinstance(parquet_path, Path) else Path(parquet_path)
    )
    output_feature_class = (
        output_feature_class
        if isinstance(output_feature_class, Path)
        else Path(output_feature_class)
    )

    # ensure will not encounter unexpected results based on incompatible input parameter or parameter combinations
    if not parquet_path.exists():
        raise ValueError(
            f"Cannot locate the input path {parquet_path}. Please double check to ensure the path is "
            f"correct and reachable."
        )

    elif parquet_path.is_dir():

        # get all the part files to start with
        pqt_prts = [prt for prt in parquet_path.rglob("part-*.parquet")]

        # now, filter based on parts being part of string - enables to specify nested partition
        if isinstance(parquet_partitions, list):
            for partition in parquet_partitions:
                pqt_prts = [prt for prt in pqt_prts if partition in str(prt)]

        # if a list is not provided, throw a fit
        elif parquet_partitions is not None:
            raise ValueError("parquet_partitions must be a list")

        # ensure we have part files still left to work with
        assert len(pqt_prts) > 0, (
            "The provided directory and partitions do not appear to contain any parquet "
            "part files."
        )
    elif parquet_path.is_file():

        assert (
            parquet_partitions is None
        ), "If providing a parquet part file, you cannot specify a parquet partition."
        assert parquet_path.stem.startswith(
            "part-"
        ), "The provided file does not appear to be a parquet part file."

        # assemble into list so iteration below works
        pqt_prts = [parquet_path]

    else:
        raise ValueError(
            "parquet_path must be either a directory for parquet data or a specific part file."
        )

    # validate the input geometry type
    if geometry_type not in geom_dict.keys():
        raise ValueError(
            f'geometry_type must be from the following list [{", ".join(geom_dict.keys())}]. You '
            f'provided "{geometry_type}".'
        )

    # create a PyArrow Table to read from
    pqt_ds = pq.ParquetDataset(parquet_path, use_legacy_dataset=False)

    # slightly change how column names are handled if using coordinates
    if isinstance(geometry_column, (tuple, list)):

        # ensure coordinate columns are in input data
        if (
            geometry_column[0] not in pqt_ds.schema.names
            or geometry_column[1] not in pqt_ds.schema.names
        ):
            raise ValueError(
                f"The geometry_column names provided for the coordinate columns do not appear to be in "
                f"the input parquet columns."
            )

        # get a list of the string column types and field aliases from parquet
        col_typ_lst, attr_alias_lst = zip(
            *[
                (str(c.type.value_type), c.name)
                if isinstance(c.type, pa.DictionaryType)
                else (str(c.type), c.name)
                for c in pqt_ds.schema
                # if c.name not in geometry_column  # geomery columns get dropped
            ]
        )

    else:

        # ensure geometry column is in input data
        if geometry_column not in pqt_ds.schema:
            raise ValueError(
                "The geometry_column does not appear to be in the input parquet columns."
            )

        # get a list of the string column types and field aliases from parquet
        col_typ_lst, attr_alias_lst = [
            (str(c.type.value_type), c.name)
            if isinstance(c.type, pa.DictionaryType)
            else str(c.type)
            for c in pqt_ds.schema
            if c.name != geometry_column
        ]

    # prepend any column names starting with a number with an 'c' and save as the field names
    attr_nm_lst = [f"c{c}" if c[0].isdigit() else c for c in attr_alias_lst]

    # use these to map to esri field types
    fld_typ_lst = [import_dtype_dict[typ] for typ in col_typ_lst]

    # check for really strange and uncaught error in naming
    if output_feature_class.name.lower().startswith("delta"):
        raise ValueError('Feature Class name cannot start with "delta".')

    # create the new feature class
    arcpy.management.CreateFeatureclass(
        out_path=str(output_feature_class.parent),
        out_name=output_feature_class.name,
        geometry_type=geom_dict[geometry_type][0],
        spatial_reference=spatial_reference,
        has_m=geom_dict[geometry_type][1],
        has_z=geom_dict[geometry_type][2],
    )

    logging.info(f"Created feature class at {str(output_feature_class)}")

    # if a schema file is provided as part of input, load it to a dict using Pandas because it's easy
    if schema_file is None:
        schema_dict = {}

    else:

        # read the csv into a Pandas DataFrame
        schema_df = pd.read_csv(schema_file)

        # swap out the string types for text so add field works
        schema_df.loc[schema_df["field_type"] == "String", "field_type"] = "TEXT"

        # dump to a dict
        schema_dict = {
            k: v for k, v in zip(schema_df["field_name"], schema_df.to_dict("records"))
        }

    # iteratively add columns using the introspected field name, alias, and type
    for nm, alias, typ in zip(attr_nm_lst, attr_alias_lst, fld_typ_lst):

        # if the field name exists in the dictionary, peel off a single field's properties and add a field using them
        if nm in schema_dict.keys():
            prop_dict = schema_dict.pop(nm)
            arcpy.management.AddField(in_table=str(output_feature_class), **prop_dict)

            # for logging progress
            log_dict = dict()
            log_dict["in_table"] = str(output_feature_class)
            log_dict = {**log_dict, **prop_dict}

        # otherwise, add based on introspected properties
        else:
            arcpy.management.AddField(
                in_table=str(output_feature_class),
                field_name=nm,
                field_type=typ,
                field_length=512,
                field_alias=alias,
                field_is_nullable="NULLABLE",
            )

            # for logging progress
            log_dict = dict(
                in_table=str(output_feature_class),
                field_name=nm,
                field_type=typ,
                field_length=512,
                field_alias=alias,
                field_is_nullable="NULLABLE",
            )

        # log progress
        logging.info(f"Field added to Feature Class {log_dict}")

    # if any fields are defined in the schema file still left over, add them
    for nm in schema_dict.keys():
        arcpy.management.AddField(
            in_table=str(output_feature_class), **schema_dict.get(nm)
        )

        # log remaining results
        log_dict = dict()
        log_dict["in_table"] = str(output_feature_class)
        log_dict = {**log_dict, **schema_dict}
        logging.info(
            f"Field added from schema file, but not detected in input data {log_dict}"
        )

    # interrogate the ACTUAL column names since, depending on the database, names can get truncated
    fc_fld_dict = {
        c.aliasName: c.name
        for c in arcpy.ListFields(str(output_feature_class))
        if c.aliasName in attr_alias_lst
    }

    # depending on the input geometry type, set the insert cursor geometry type
    if geometry_type == "COORDINATES":
        insert_geom_typ = "SHAPE@XY"
    else:
        insert_geom_typ = "SHAPE@WKB"

    # create the list of feature class columns for the insert cursor and for row lookup from parquet from pydict object
    insert_col_lst = list(fc_fld_dict.values()) + [insert_geom_typ]

    if geometry_type == "COORDINATES":
        pydict_col_lst = list(fc_fld_dict.keys())
    else:
        pydict_col_lst = list(fc_fld_dict.keys()) + [geometry_column]

    # this prevents pyarrow from getting hung up
    arcpy.env.autoCancelling = False

    # set up so progress is communicated to user
    arcpy.SetProgressorLabel("Importing rows...")

    # variable to track completed count
    added_cnt = 0

    # variable to track fail count
    fail_cnt = 0

    # create a cursor for inserting rows
    with arcpy.da.InsertCursor(
        str(output_feature_class), insert_col_lst
    ) as insert_cursor:

        # partition replacement values dictionary
        partition_values_dict = {}

        # flag for if at sample count and need to break out of loop
        at_sample_count = False

        # variable to track start time
        start_time = time.time()

        # iterate the parquet part files
        for part_file in pqt_prts:

            # load into a PyArrow Table - thankfully partition aware
            pa_tbl = pq.read_table(part_file)

            # pull the parquet data into a dict
            pqt_pydict = pa_tbl.to_pydict()

            # for every row index in the number of rows
            for pqt_idx in range(pa_tbl.num_rows):

                # instantiate the row variable so error messages can be formatted.
                row = None

                # try to add the row
                try:

                    # start creating a dict of single key partition_column_list pairs for this row of data_dir
                    row_pydict = {k: v[pqt_idx] for k, v in pqt_pydict.items()}

                    # populate the row dictionary with values from the partition dict
                    row_dict = {k: row_pydict.get(k) for k in pydict_col_lst}

                    # fill any partition values
                    for p_key in partition_values_dict.keys():
                        row_dict[p_key] = partition_values_dict[p_key]

                    # if the geometry is being generated from coordinate columns, create the coordinate tuple
                    if geometry_type == "COORDINATES":
                        row_dict[insert_geom_typ] = (
                            row_pydict[geometry_column[0]],
                            row_pydict[geometry_column[1]],
                        )

                    # create a row object by plucking out the values from the row dictionary
                    row = tuple(row_dict.values())

                    # insert the row
                    insert_cursor.insertRow(row)

                    # update the completed count
                    added_cnt += 1

                # if cannot add the row
                except Exception as e:

                    # handle case of having issues prior to even getting the row
                    if row is None:
                        raise

                    else:

                        # update the fail count
                        fail_cnt += 1

                        # make sure the reason is tracked
                        logging.warning(
                            f"Could not import row.\n\nContents:{row}\n\nMessage: {e}"
                        )

                # check of at sample count
                if added_cnt == sample_count:
                    at_sample_count = True
                    break

                # provide status updates every 1000 features, and provide an exit if cancelled
                if added_cnt % 1000 == 0:

                    arcpy.SetProgressorLabel(f"Imported {added_cnt:,} rows...")

                    if arcpy.env.isCancelled:
                        break

                # provide messages every 10,000 features
                if added_cnt % 10000 == 0:

                    # find the elapsed time
                    elapsed_time = time.time() - start_time

                    # calculate the rate per hour
                    rate = round(added_cnt / elapsed_time * 3600)

                    logging.info(
                        f"Imported {added_cnt:,} rows at a rate of {rate:,} per hour..."
                    )

            # ensure next batch is not run if cancelled or only running a sample
            if arcpy.env.isCancelled or at_sample_count:
                break

    # declare success, and track failure if necessary
    success_msg = f"Successfully imported {added_cnt:,} rows."
    arcpy.SetProgressorLabel(success_msg)
    arcpy.ResetProgressor()
    logging.info(success_msg)

    if fail_cnt > 0:
        fail_msg = f"Failure count: {fail_cnt:,}"
        logging.warning(fail_msg)

    # if compacting, do it
    if compact:
        arcpy.SetProgressorLabel("Compacting data.")
        arcpy.management.Compact(str(output_feature_class.parent))
        logging.info("Successfully compacted data.")

    # build spatial index if requested
    if build_spatial_index:
        arcpy.SetProgressorLabel("Building spatial index.")
        arcpy.management.AddSpatialIndex(str(output_feature_class))
        logging.info("Completed building spatial index.")

    return output_feature_class


def create_schema_file(
    template_feature_class_path: Path, output_schema_file: Path
) -> Path:
    """
    Create a CSV Schema file to use with ``parquet_to_feature_class``.

    Args:
        template_feature_class_path: Path to Feature Class with the schema to mimic.
        output_schema_file: Path where the CSV schema file will be stored.
    """
    # read the fields from the feature class
    fld_lst = [
        fld
        for fld in arcpy.ListFields(str(template_feature_class_path))
        if fld.type != "OID" and fld.type != "Geometry"
    ]

    # properties and keys to use
    desc_keys = [
        "name",
        "type",
        "precision",
        "scale",
        "length",
        "aliasName",
        "isNullable",
        "required",
        "domain",
    ]
    param_keys = [
        "field_name",
        "field_type",
        "field_precision",
        "field_scale",
        "field_length",
        "field_alias",
        "field_is_nullable",
        "field_is_required",
        "field_domain",
    ]

    # dump all the properties into a list of property sets
    prop_lst = [[getattr(fld, k) for k in desc_keys] for fld in fld_lst]

    # create a dataframe to make creating the CSV easier
    schema_df = pd.DataFrame(prop_lst, columns=param_keys)

    # write to a file
    schema_df.to_csv(output_schema_file, encoding="utf-8", index=False)

    return output_schema_file
