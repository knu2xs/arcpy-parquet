from pathlib import Path
from importlib.util import find_spec
import logging
from typing import Optional, Union

__all__ = ["has_arcpy", "has_pandas", "has_pyspark"]

# provide variable indicating if arcpy is available
has_arcpy: bool = find_spec("arcpy") is not None

# provide variable indicating if pandas is available
has_pandas: bool = find_spec("pandas") is not None

# provide variable indicating if PySpark is available
has_pyspark: bool = find_spec("pyspark") is not None
