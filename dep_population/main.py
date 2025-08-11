import json
import sys
from typing import Annotated

from dep_tools.aws import object_exists
from dep_tools.grids import grid, gadm
from dep_tools.stac_utils import StacCreator
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter
from dep_tools.namers import S3ItemPath
from odc.geo.xr import xr_reproject
import pandas as pd
import typer
import xarray as xr

from loader import country_codes_for_area, load_population_counts
from processor import population_density

app = typer.Typer()

BUCKET = "dep-public-staging"
DATASET_ID = "population"
VERSION = "0.1.1"
DATETIME = "2023_2025"

ITEMPATH = S3ItemPath(
    bucket=BUCKET,
    sensor="pdhhdx",
    dataset_id=DATASET_ID,
    version=VERSION,
    time=DATETIME,
)


def population_grid():
    # Yes, this is convoluted, see
    pop_grid = pd.DataFrame.from_records(
        grid(resolution=100, intersect_with=gadm()),
        columns=["index", "geobox"],
        index="index",
    )
    pop_grid.index = pd.MultiIndex.from_tuples(pop_grid.index)
    pop_grid = pop_grid.geobox
    return pop_grid


def parse_tile_id(tile_str) -> tuple[int, ...]:
    # "[12, 345]" -> [12,345]
    return tuple(int(value) for value in tile_str[1:-1].split(","))


@app.command()
def run_task(tile_id: Annotated[str, typer.Option(parser=parse_tile_id)]):
    writer = AwsDsCogWriter(ITEMPATH)
    stac_creator = StacCreator(ITEMPATH)
    stac_writer = AwsStacWriter(ITEMPATH)

    area = population_grid().loc[tile_id]
    pop_density = []
    for code in country_codes_for_area(area):
        pop_count = load_population_counts(code, area)
        if pop_count is not None:  # no data for this area
            country_pop_density = population_density(pop_count)
            # Only reproject now since densities are not cell-area specific
            country_pop_density_reproj = xr_reproject(country_pop_density, area)
            pop_density.append(country_pop_density_reproj)

    if len(pop_density) > 1:
        output = xr.concat(pop_density, dim="z").max(dim="z")
    else:
        output = pop_density[0]

    output = output.to_dataset(name="pop_per_sqkm").astype("float32")

    writer.write(output, tile_id)

    stac_item = stac_creator.process(output, tile_id)
    stac_writer.write(stac_item, tile_id)


@app.command()
def print_ids():
    json.dump(
        [
            tile_id
            for tile_id in population_grid().index
            if not object_exists(bucket=BUCKET, key=ITEMPATH.stac_path(tile_id))
        ],
        sys.stdout,
    )


if __name__ == "__main__":
    app()
