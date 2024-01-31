from pathlib import Path
import tempfile

import arcpy
import pytest

arcpy.env.overwriteOutput = True


@pytest.fixture(scope="session")
def tmp_dir():

    # get a temporary directory to work with for testing
    dir = tempfile.mkdtemp()

    # provide the directory during testing
    yield Path(dir)

    # destroy when session is finished - using arcpy since less schema lock issues
    arcpy.management.Delete(dir)


@pytest.fixture(scope="session")
def tmp_gdb(tmp_dir):

    # get a file geodatabase in the temporary directory
    fgdb = arcpy.management.CreateFileGDB(str(tmp_dir), "temp.gdb")[0]

    # provide the path to the FGDB during testing
    yield Path(fgdb)

    # remove at end of session
    arcpy.Delete_management(fgdb)
