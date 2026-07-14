from importlib.util import find_spec
import re
import unicodedata

__all__ = ["has_arcpy", "has_pandas", "has_pyspark", "slugify"]

# provide variable indicating if arcpy is available
has_arcpy: bool = find_spec("arcpy") is not None

# provide variable indicating if pandas is available
has_pandas: bool = find_spec("pandas") is not None

# provide variable indicating if PySpark is available
has_pyspark: bool = find_spec("pyspark") is not None


def slugify(value: str, separator: str = "_") -> str:
	"""Convert input text to a filesystem-safe identifier.

	Args:
		value: Input text.
		separator: Separator for word boundaries.

	Returns:
		Normalized, lowercase slug using ASCII-safe characters.
	"""
	normalized = unicodedata.normalize("NFKD", value)
	ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
	cleaned = re.sub(r"[^A-Za-z0-9]+", separator, ascii_text.strip().lower())
	collapsed = re.sub(rf"{re.escape(separator)}+", separator, cleaned)
	return collapsed.strip(separator)
