import json
import sys
from typing import Annotated

from dep_tools.aws import object_exists
from dep_tools.grids import grid, gadm
from dep_tools.stac_utils import StacCreator
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter
from dep_tools.namers import S3ItemPath
import pandas as pd
import typer

from loader import country_codes_for_area, load_population_counts
from processor import population_density

app = typer.Typer()

BUCKET = "dep-public-staging"
DATASET_ID = "population"
VERSION = "0.1.0"
DATETIME = "2023_2025"

ITEMPATH = S3ItemPath(
    bucket=BUCKET,
    sensor="combination",
    dataset_id=DATASET_ID,
    version=VERSION,
    time=DATETIME,
)


def population_grid():
    # Yes, this is convoluted, see
    pop_grid = pd.DataFrame.from_records(
        grid(intersect_with=gadm()), columns=["index", "geobox"], index="index"
    )
    pop_grid.index = pd.MultiIndex.from_tuples(pop_grid.index)
    pop_grid = pop_grid.geobox
    return pop_grid


def parse_tile_id(tile_str) -> tuple[int, ...]:
    return tuple(int(value) for value in tile_str[1:-1].split(","))


@app.command()
def run_task(tile_id: Annotated[str, typer.Option(parser=parse_tile_id)]):
    writer = AwsDsCogWriter(ITEMPATH)
    stac_creator = StacCreator(ITEMPATH)
    stac_writer = AwsStacWriter(ITEMPATH)

    area = population_grid().loc[tile_id]
    for code in country_codes_for_area(area):
        pop_count = load_population_counts(code)
        pop_density = population_density(pop_count).to_dataset(name="people_per_sqm")

        writer.write(pop_density, tile_id)

        stac_item = stac_creator.process(pop_density, tile_id)
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
