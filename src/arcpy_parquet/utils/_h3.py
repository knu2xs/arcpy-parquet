import json
from typing import Optional

import arcpy


def _to_wgs84(geom: arcpy.Geometry) -> arcpy.Geometry:
    """Project geometry to WGS84 when possible for H3 operations."""
    sr = getattr(geom, "spatialReference", None)
    if sr and getattr(sr, "factoryCode", None) == 4326:
        return geom
    try:
        return geom.projectAs(arcpy.SpatialReference(4326))
    except Exception:
        return geom


def _geometry_to_geojson(geom: arcpy.Geometry) -> dict:
    """Convert ArcPy geometry to a GeoJSON-like mapping."""
    if hasattr(geom, "__geo_interface__"):
        return geom.__geo_interface__

    esri_json = json.loads(geom.JSON)

    if "x" in esri_json and "y" in esri_json:
        return {
            "type": "Point",
            "coordinates": [esri_json["x"], esri_json["y"]],
        }

    if "paths" in esri_json:
        paths = [
            [[pt[0], pt[1]] for pt in path]
            for path in esri_json.get("paths", [])
            if len(path) > 0
        ]
        if len(paths) == 1:
            return {"type": "LineString", "coordinates": paths[0]}
        return {"type": "MultiLineString", "coordinates": paths}

    if "rings" in esri_json:
        rings = [
            [[pt[0], pt[1]] for pt in ring]
            for ring in esri_json.get("rings", [])
            if len(ring) > 0
        ]
        return {"type": "Polygon", "coordinates": rings}

    raise ValueError("Unsupported geometry encountered for H3 export.")


def _line_cells_from_geojson(
    line_geojson: dict, h3_module, h3_resolution: int
) -> list[str]:
    """Generate H3 cells intersecting a line or multiline."""
    if line_geojson["type"] == "LineString":
        line_groups = [line_geojson["coordinates"]]
    elif line_geojson["type"] == "MultiLineString":
        line_groups = line_geojson["coordinates"]
    else:
        raise ValueError(
            f"Unsupported line GeoJSON type '{line_geojson['type']}' for H3 line export."
        )

    cells = set()
    for line in line_groups:
        if len(line) == 0:
            continue

        for lon, lat in line:
            cells.add(h3_module.latlng_to_cell(lat, lon, h3_resolution))

        for (lon_1, lat_1), (lon_2, lat_2) in zip(line, line[1:]):
            cell_1 = h3_module.latlng_to_cell(lat_1, lon_1, h3_resolution)
            cell_2 = h3_module.latlng_to_cell(lat_2, lon_2, h3_resolution)

            if hasattr(h3_module, "grid_path_cells"):
                try:
                    cells.update(h3_module.grid_path_cells(cell_1, cell_2))
                except Exception:
                    cells.add(cell_1)
                    cells.add(cell_2)

    return sorted(cells)


def h3_value_from_geometry(
    geom: Optional[arcpy.Geometry],
    input_shape_type: str,
    h3_module,
    h3_resolution: int,
) -> Optional[str]:
    """Convert geometry to H3 value(s) encoded as string."""
    if geom is None:
        return None

    geom_wgs84 = _to_wgs84(geom)
    shp_type = input_shape_type.upper()

    if shp_type == "POINT":
        pt = geom_wgs84.centroid
        return str(h3_module.latlng_to_cell(pt.Y, pt.X, h3_resolution))

    if shp_type == "MULTIPOINT":
        cells = set()
        for part in geom_wgs84:
            for pt in part:
                if pt is None:
                    continue
                cells.add(h3_module.latlng_to_cell(pt.Y, pt.X, h3_resolution))
        return json.dumps(sorted(cells))

    geojson_geom = _geometry_to_geojson(geom_wgs84)

    if shp_type == "POLYLINE":
        return json.dumps(
            _line_cells_from_geojson(geojson_geom, h3_module, h3_resolution)
        )

    if shp_type == "POLYGON":
        h3_shape = h3_module.geo_to_h3shape(geojson_geom)
        cells = sorted(h3_module.h3shape_to_cells(h3_shape, h3_resolution))
        return json.dumps(cells)

    raise ValueError(f"Unsupported shape type '{input_shape_type}' for H3 export.")
