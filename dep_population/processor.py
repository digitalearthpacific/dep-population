import numpy as np
from pyproj import CRS, Geod
from shapely.geometry import box
import xarray as xr


def population_density(population_count: xr.DataArray):
    area_sqm = area_raster(population_count)
    sqkm_per_sqm = 1 / (1000 * 1000)
    area_sqkm = area_sqm * sqkm_per_sqm
    return population_count / area_sqkm


def area_raster(da: xr.DataArray) -> xr.DataArray:
    if CRS.from_user_input(da.odc.crs).is_geographic:
        return area_raster_4326(da)
    else:
        return xr.full_like(da, abs(np.prod(da.rio.resolution())).astype("float32"))


def area_raster_4326(da: xr.DataArray) -> xr.DataArray:
    """Calculate area for DataArrays in EPSG:4326, i.e. latlong. This function
    works by creating a polygon for each cell and calculating the area using
    `pyproj.geod.geometry_area_perimeter`.
    """
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
