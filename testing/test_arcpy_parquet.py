"""
This is a stubbed out test file designed to be used with PyTest, but can 
easily be modified to support any testing framework.
"""

from pathlib import Path
import sys
import tempfile

import arcpy
import pyarrow.parquet as pq


# get paths to useful resources - notably where the src directory is
self_pth = Path(__file__)
dir_test = self_pth.parent
dir_prj = dir_test.parent
dir_src = dir_prj / "src"

# insert the src directory into the path and import the projct package
sys.path.insert(0, str(dir_src))
import arcpy_parquet

test_fc = Path(r"D:\projects\white-pass-skiing\data\interim\interim.gdb\ski_runs_wgs84")


def test_feature_class_to_parquet():

    tmp_pqt = Path(tempfile.gettempdir()) / "test.parquet"

    out_pqt = arcpy_parquet.feature_class_to_parquet(test_fc, tmp_pqt)

    assert out_pqt.exists()

    pqt_ds = pq.ParquetDataset(tmp_pqt, use_legacy_dataset=False)
    assert "wkb" in pqt_ds.schema.names


def test_feature_class_to_parquet_countries():

    countries_fc = r"D:\projects\ba-data-engineering\data\raw\MBR_Countries_World_raw.gdb\MBR_Countries_World_raw_1022"
    out_pqt = (
        r"D:\projects\ba-data-engineering\data\external\countries_mbr_raw_2023.parquet"
    )

    res = arcpy_parquet.feature_class_to_parquet(Path(countries_fc), Path(out_pqt))

    assert res.exists()


def test_parquet_to_feature_class_basemaps():
    import arcpy

    arcpy.env.overwriteOutput = True
    in_pqt = Path(
        r"D:\projects\ba-data-engineering\data\processed\delivery\foursquare_basemaps.parquet"
    )
    out_fc = Path(
        r"D:\projects\ba-data-engineering\data\processed\delivery\foursquare_basemaps_devsummit.gdb\places"
    )
    schma_csv = Path(
        r"D:\projects\ba-data-engineering\data\processed\delivery\foursquare_basemaps_schema.csv"
    )
    res = arcpy_parquet.parquet_to_feature_class(
        in_pqt,
        output_feature_class=out_fc,
        schema_file=schma_csv,
        build_spatial_index=True,
    )[0]
    assert out_fc.exists()


def test_parquet_to_feature_class_h3():
    import arcpy

    arcpy.env.overwriteOutput = True
    in_pqt = Path(
        r"D:\projects\foursquare-processing\data\raw\foursquare_deltas\esri_h3_05\parquet"
    )
    out_fc = Path(
        r"D:\projects\foursquare-processing\data\processed\processed.gdb\adu_delta_h3_05"
    )
    schma_csv = r"D:\projects\foursquare-processing\data\raw\foursquare_deltas\esri_h3_05\schema\part-00000-0445cd8d-f968-460a-9b4c-d41e66bb1de1-c000.csv"
    res = arcpy_parquet.parquet_to_feature_class(
        in_pqt,
        output_feature_class=out_fc,
        schema_file=schma_csv,
        geometry_type="POLYGON",
        build_spatial_index=True,
    )
    assert arcpy.Exists(str(out_fc))


def test_parquet_to_feature_class_coordinates(tmp_gdb):
    in_pqt = Path(r"D:\projects\foursquare-processing\data\sample\coordinate_data")
    out_fc = tmp_gdb / "test_coordinates"
    res = arcpy_parquet.parquet_to_feature_class(
        in_pqt,
        out_fc,
        geometry_type="COORDINATES",
        geometry_column=["longitude", "latitude"],
    )
    assert arcpy.Exists(str(out_fc))
