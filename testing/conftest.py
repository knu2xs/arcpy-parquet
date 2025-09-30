from pathlib import Path
import tempfile

import arcpy
import pytest

arcpy.env.overwriteOutput = True


@pytest.fixture(scope="session")
def tmp_dir():
    tmpdir = tempfile.TemporaryDirectory()
    tmpdir_pth = Path(tmpdir.name)
    yield tmpdir_pth
    tmpdir.cleanup()


@pytest.fixture(scope="function")
def tmp_pqt():
    tmpdir = tempfile.TemporaryDirectory(suffix=".parquet")
    tmpdir_pth = Path(tmpdir.name)
    yield tmpdir_pth
    tmpdir.cleanup()


@pytest.fixture(scope="session")
def tmp_gdb(tmp_dir):
    # get a file geodatabase in the temporary directory
    fgdb = arcpy.management.CreateFileGDB(str(tmp_dir), "temp.gdb")[0]

    # provide the path to the FGDB during testing
    yield Path(fgdb)

    # remove at end of session
    arcpy.Delete_management(fgdb)
