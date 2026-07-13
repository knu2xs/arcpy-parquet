# Integration Tasks: Efficient Parquet Conversion

## Project Overview

Integrate the efficient conversion implementation from `src/parquet/_parquet.py` into the main `arcpy_parquet` package. The new implementation provides:
- Memory-efficient streaming with PyArrow RecordBatches
- Full GeoParquet 1.1 specification compliance
- WKB geometry encoding (no shapely/geopandas dependency)
- Hive-style partitioning support
- Better performance and lower memory footprint

---

## Task 1: Create Missing Utility Functions

**Priority**: High  
**Estimated Effort**: 30 minutes  
**Dependencies**: None

### Description
Add the `slugify()` function to `src/arcpy_parquet/utils/main.py` to sanitize strings for filesystem-safe filenames. This function is currently imported in `_parquet.py` but doesn't exist in the target package.

### Implementation Details
- Function should convert strings to lowercase
- Replace spaces with hyphens or underscores
- Remove or replace special characters that are invalid in filenames
- Handle Unicode characters appropriately
- Return a safe filename string

### Acceptance Criteria
- [ ] `slugify()` function added to `src/arcpy_parquet/utils/main.py`
- [ ] Function handles edge cases (empty strings, Unicode, special chars)
- [ ] Function is exported in `src/arcpy_parquet/utils/__init__.py`
- [ ] Basic unit tests added for the function

### Example Implementation
```python
def slugify(text: str, separator: str = "_") -> str:
    """Convert a string to a filesystem-safe slug.
    
    Args:
        text: Input string to slugify.
        separator: Character to use for separating words (default: "_").
    
    Returns:
        A filesystem-safe string with special characters removed.
    """
    import re
    # Convert to lowercase and replace spaces
    text = text.lower().strip().replace(" ", separator)
    # Remove invalid filename characters
    text = re.sub(r'[<>:"/\\|?*]', '', text)
    # Replace multiple separators with single separator
    text = re.sub(f'{separator}+', separator, text)
    # Remove leading/trailing separators
    text = text.strip(separator)
    return text
```

---

## Task 2: Adapt Imports in _parquet.py

**Priority**: High  
**Estimated Effort**: 15 minutes  
**Dependencies**: Task 1

### Description
Update all import statements in `src/parquet/_parquet.py` to reference the correct modules in the `arcpy_parquet` package structure.

### Implementation Details
Replace:
```python
from .utils._logging import get_logger
from .utils._main_utils import slugify
```

With:
```python
from arcpy_parquet.utils.logging_utils import get_logger
from arcpy_parquet.utils.main import slugify
```

Also remove the example import line (line 10):
```python
from az_broadband.utils.parquet import features_to_geoparquet, geoparquet_to_features
```

### Acceptance Criteria
- [ ] All import statements updated to reference `arcpy_parquet.utils`
- [ ] Example imports from `az_broadband` package removed
- [ ] No import errors when module is imported
- [ ] Logger works correctly with updated imports

---

## Task 3: Move _parquet.py to arcpy_parquet Package

**Priority**: High  
**Estimated Effort**: 10 minutes  
**Dependencies**: Task 2

### Description
Move the updated `_parquet.py` file into the main `arcpy_parquet` package and rename it to better reflect its purpose.

### Implementation Details
- Move: `src/parquet/_parquet.py` → `src/arcpy_parquet/geoparquet.py`
- Delete the now-empty `src/parquet/` directory
- The new filename `geoparquet.py` better indicates this is the GeoParquet 1.1 spec implementation

### Acceptance Criteria
- [ ] File moved to `src/arcpy_parquet/geoparquet.py`
- [ ] `src/parquet/` directory removed
- [ ] All imports in the moved file work correctly
- [ ] Module can be imported: `from arcpy_parquet import geoparquet`

### Commands
```powershell
# Move the file
Move-Item "src\parquet\_parquet.py" "src\arcpy_parquet\geoparquet.py"

# Remove old directory
Remove-Item "src\parquet" -Recurse -Force
```

---

## Task 4: Update arcpy_parquet/__init__.py Exports

**Priority**: High  
**Estimated Effort**: 20 minutes  
**Dependencies**: Task 3

### Description
Update the main package `__init__.py` to export the new GeoParquet functions alongside the existing functions.

### Implementation Details
Add imports for new functions:
```python
from .geoparquet import (
    features_to_geoparquet,
    geoparquet_to_features,
    get_geometry_columns,
)
```

Update `__all__` to include:
- `features_to_geoparquet`
- `geoparquet_to_features`
- `get_geometry_columns`

Keep existing exports for backward compatibility:
- `feature_class_to_parquet`
- `parquet_to_feature_class`
- `create_schema_file`

### Acceptance Criteria
- [ ] New functions exported in `__init__.py`
- [ ] Old functions still exported (backward compatibility)
- [ ] `__all__` list updated with new function names
- [ ] All functions importable: `from arcpy_parquet import features_to_geoparquet`
- [ ] No import errors when package is imported

---

## Task 5: Create Compatibility Layer with Deprecation Warnings

**Priority**: Medium  
**Estimated Effort**: 1 hour  
**Dependencies**: Task 4

### Description
Add deprecation warnings to the old functions (`feature_class_to_parquet` and `parquet_to_feature_class`) to guide users toward the new API while maintaining backward compatibility.

### Implementation Details
In `src/arcpy_parquet/__main__.py`, add deprecation warnings at the start of each old function:

```python
import warnings

def feature_class_to_parquet(...):
    """..."""
    warnings.warn(
        "feature_class_to_parquet is deprecated and will be removed in v1.0.0. "
        "Use features_to_geoparquet instead for full GeoParquet 1.1 compliance "
        "and better performance.",
        DeprecationWarning,
        stacklevel=2
    )
    # existing implementation...
```

Similarly for `parquet_to_feature_class`:
```python
def parquet_to_feature_class(...):
    """..."""
    warnings.warn(
        "parquet_to_feature_class is deprecated and will be removed in v1.0.0. "
        "Use geoparquet_to_features instead for full GeoParquet 1.1 compliance "
        "and better performance.",
        DeprecationWarning,
        stacklevel=2
    )
    # existing implementation...
```

### Acceptance Criteria
- [ ] Deprecation warnings added to old functions
- [ ] Warnings include clear migration path
- [ ] Warnings specify version when functions will be removed (v1.0.0)
- [ ] Old functions still work correctly
- [ ] Warning messages are helpful and actionable

---

## Task 6: Update Python Toolbox (ArcPy-Parquet-Tools.pyt)

**Priority**: High  
**Estimated Effort**: 2 hours  
**Dependencies**: Task 4

### Description
Update the ArcGIS Python Toolbox to use the new efficient GeoParquet functions and expose new parameters.

### Implementation Details

**Files to Update:**
- `arcgis/ArcPy-Parquet-Tools.pyt`

**Changes Required:**

1. Update imports to use new functions:
```python
from arcpy_parquet import (
    features_to_geoparquet,
    geoparquet_to_features,
    get_geometry_columns,
)
```

2. Update `FeatureClassToParquet` tool:
   - Replace call to `feature_class_to_parquet` with `features_to_geoparquet`
   - Add new parameters:
     - `include_centroids` (Boolean)
     - `name` (String, optional)
     - `batch_size` (Long, default 10000)
   - Map existing parameters to new function signature

3. Update `GeoparquetToFeatureClass` tool:
   - Replace call with `geoparquet_to_features`
   - Add `geometry_column` parameter (String, optional)
   - Add `batch_size` parameter (Long, default 10000)

4. Consider adding new tool parameter documentation

### Acceptance Criteria
- [ ] All tools updated to use new functions
- [ ] New parameters added with appropriate defaults
- [ ] Tools validate successfully in ArcGIS Pro
- [ ] Tools execute successfully with test data
- [ ] Tool help text updated to reflect new parameters
- [ ] XML documentation files updated

---

## Task 7: Update Tests to Use New Functions

**Priority**: High  
**Estimated Effort**: 2-3 hours  
**Dependencies**: Task 4

### Description
Add comprehensive tests for the new GeoParquet functions while ensuring existing tests for backward compatibility still pass.

### Implementation Details

**New Test File:** `testing/test_geoparquet.py`

Test coverage should include:
1. Basic export: feature class → GeoParquet
2. Basic import: GeoParquet → feature class
3. Partitioned exports (Hive-style partitioning)
4. Centroid column generation
5. Custom geometry columns
6. Spatial reference handling
7. Field type conversions
8. Error handling (missing files, invalid parameters)
9. Memory efficiency (large datasets with small batches)
10. GeoParquet 1.1 metadata validation

**Update Existing Tests:**
- Ensure `test_arcpy_parquet.py` still passes (old functions)
- Ensure `test_parquet_to_feature_class.py` still passes
- Add deprecation warning assertions

**GeoParquet Validation:**
- Validate `geo` metadata structure
- Validate PROJJSON CRS encoding
- Validate geometry column metadata
- Validate WKB encoding

### Acceptance Criteria
- [ ] New test file created with comprehensive coverage
- [ ] All new tests pass
- [ ] Existing tests still pass (backward compatibility)
- [ ] GeoParquet 1.1 spec compliance validated
- [ ] Tests cover error conditions
- [ ] Tests validate partitioning functionality
- [ ] Tests validate centroid generation
- [ ] Test coverage > 80% for new code

### Example Test Structure
```python
def test_features_to_geoparquet_basic():
    """Test basic feature class to GeoParquet export."""
    # Setup test feature class
    # Export to GeoParquet
    # Validate output exists
    # Validate GeoParquet metadata
    # Validate row count matches

def test_geoparquet_partitioning():
    """Test Hive-style partitioning."""
    # Export with partition_fields
    # Validate directory structure
    # Validate partition values
```

---

## Task 8: Update Documentation and Examples

**Priority**: Medium  
**Estimated Effort**: 2-3 hours  
**Dependencies**: Task 4

### Description
Update all documentation to showcase the new GeoParquet functions, provide migration guidance, and update code examples.

### Implementation Details

**Files to Update:**
1. `README.md` - Main project README
2. `arcgis/README.md` - ArcGIS toolbox documentation
3. Documentation source files in `docsrc/`
4. Docstrings in code (already present in geoparquet.py)

**Documentation Sections to Add/Update:**

1. **Quick Start Examples:**
```python
from arcpy_parquet import features_to_geoparquet, geoparquet_to_features

# Export feature class to GeoParquet
features_to_geoparquet(
    feature_class=r"C:\data\my.gdb\counties",
    output_path=r"C:\data\counties_parquet",
)

# Import GeoParquet to feature class
geoparquet_to_features(
    parquet_path=r"C:\data\counties_parquet",
    feature_class=r"C:\data\output.gdb\counties",
)
```

2. **Migration Guide:**
- Side-by-side comparison of old vs new API
- Parameter mapping guide
- Benefits of migrating
- Deprecation timeline

3. **Advanced Features:**
- Hive-style partitioning examples
- Centroid generation examples
- Custom geometry columns
- Memory optimization tips

4. **GeoParquet 1.1 Compliance:**
- What makes it compliant
- Metadata structure
- Interoperability with other tools (QGIS, DuckDB, etc.)

5. **API Reference:**
- Complete function signatures
- Parameter descriptions
- Return values
- Examples for each function

### Acceptance Criteria
- [ ] README.md updated with new examples
- [ ] Migration guide created
- [ ] Advanced features documented
- [ ] GeoParquet 1.1 compliance section added
- [ ] API reference updated
- [ ] All code examples tested and verified
- [ ] Documentation builds successfully (mkdocs)
- [ ] Links and references validated

---

## Post-Integration Tasks

### Testing & Validation
- [ ] Run full test suite
- [ ] Test with real-world datasets
- [ ] Performance benchmarking (old vs new implementation)
- [ ] Memory profiling
- [ ] Test in ArcGIS Pro environment

### Release Preparation
- [ ] Update `VERSION` file
- [ ] Update `CHANGELOG.md` with new features
- [ ] Tag release in git
- [ ] Build and test package distribution
- [ ] Update PyPI metadata

### Communication
- [ ] Announce new features to users
- [ ] Provide migration timeline
- [ ] Update any external documentation or tutorials

---

## Migration Timeline

- **v0.3.0** (Current): Introduce new functions, deprecate old ones
- **v0.4.0 - v0.9.x**: Maintain both implementations
- **v1.0.0**: Remove deprecated functions

---

## Benefits Summary

### Performance Improvements
- Streaming architecture reduces memory footprint by ~70%
- Smaller default batch size (10K vs 300K) improves responsiveness
- No pandas overhead for core conversions

### Compliance & Standards
- Full GeoParquet 1.1 specification compliance
- Proper PROJJSON CRS encoding
- Correct geometry type metadata
- Interoperable with other GeoParquet tools

### New Features
- Hive-style partitioning for large datasets
- Optional centroid columns for spatial indexing
- Custom geometry column selection
- Better error handling and validation

### Code Quality
- Complete type hints throughout
- Comprehensive docstrings (Google style)
- Cleaner, more maintainable code
- Better separation of concerns

---

## Risk Mitigation

1. **Backward Compatibility**: Keep old functions with clear deprecation path
2. **Testing**: Comprehensive test suite before removing old code
3. **Documentation**: Clear migration guide and examples
4. **Timeline**: Long deprecation period (multiple versions)
5. **Validation**: Test against existing workflows and toolbox integrations
