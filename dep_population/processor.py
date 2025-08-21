import numpy as np
from pyproj import CRS, Geod
from shapely.geometry import box
import xarray as xr


def population_density(population_count: xr.DataArray) -> xr.DataArray:
    """Convert the given population counts to densities

    Args:
        population_count: A DataArray where each cell contains the 
        population count in that location.

    Returns:
        Density in persons / square kilometer.
    """
    area_sqm = area_raster(population_count)
    sqkm_per_sqm = 1 / (1000 * 1000)
    area_sqkm = area_sqm * sqkm_per_sqm
    return population_count / area_sqkm


def area_raster(da: xr.DataArray) -> xr.DataArray:
    """Calculate area for each cell in a DataArray.

    For geographic projections, a polygon is created for each cell
    and area calculated using `pyproj.geod.geometry_area_perimeter`.

    Args:
        da: A DataArray with x & y coordinates. The footprint, resolution
        (odc.geobox.resolution) and projection (odc.crs) is used for the
        output raster.

    Returns:
        Output values reflect the area of each pixel in square meters.
    """
    if CRS.from_user_input(da.odc.crs).is_geographic:
        # Cell area varies based on latitude
        return _area_raster_4326(da)
    else:
        # Area is just the squared resolution
        return xr.full_like(da, abs(np.prod(da.rio.resolution())).astype("float32"))


def _area_raster_4326(da: xr.DataArray) -> xr.DataArray:
    geod = Geod(ellps="WGS84")
    an_x = float(da.x[0])
    res = da.odc.geobox.resolution

    area_da_for_y = xr.DataArray(
        [
            abs(
                geod.geometry_area_perimeter(
                    box(
                        an_x - res.x / 2,
                        y - res.y / 2,
                        an_x + res.x / 2,
                        y + res.y / 2,
                    )
                )[0]
            )
            for y in da.y
        ],
        coords=dict(y=da.y),
        dims="y",
    )

    return area_da_for_y * xr.ones_like(da)
