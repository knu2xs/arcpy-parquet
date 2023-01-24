"""
This is a stubbed out test file designed to be used with PyTest, but can 
easily be modified to support any testing framework.
"""

from pathlib import Path
import sys
import tempfile

import arcpy

# get paths to useful resources - notably where the src directory is
self_pth = Path(__file__)
dir_test = self_pth.parent
dir_prj = dir_test.parent
dir_src = dir_prj/'src'

# insert the src directory into the path and import the projct package
sys.path.insert(0, str(dir_src))
import arcpy_parquet

test_fc = Path(r'D:\projects\white-pass-skiing\data\interim\interim.gdb\ski_runs_wgs84')


def test_feature_class_to_parquet():

    tmp_pqt = Path(tempfile.gettempdir())/'test.parquet'

    out_pqt = arcpy_parquet.feature_class_to_parquet(test_fc, tmp_pqt)

    assert out_pqt.exists()
