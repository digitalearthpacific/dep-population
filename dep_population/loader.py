from dep_tools.grids import gadm
from dep_tools.loaders import Loader
from dep_tools.searchers import Searcher
from odc.geo.geobox import GeoBox
import requests
from rasterio.io import MemoryFile, ZipMemoryFile
import rioxarray as rx
import xarray as xr


def country_codes_for_area(area: GeoBox) -> list[str]:
    return gadm().to_crs(area.crs).clip(area.boundingbox.bbox).GID_0.unique().tolist()


def load_population_counts(country_code: str) -> xr.DataArray:
    worldpop_codes = ["ASM", "GUM", "MNP", "NCL", "PCN", "PNG", "PYF", "TKL", "TON"]

    direct_downloads = dict(
        COK="https://pacificdata.org/data/dataset/e42508ea-22db-46d7-a00e-74fb4c4b7c8b/resource/79a4827c-efe8-40b4-ac28-2f2d0a4cafdf/download/COK_t_pop_2025.tif",
        FJI="https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_population/raw/Fiji+Population+Grids+2023.zip",
        FSM="https://pacificdata.org/data/dataset/4c3857eb-5d8c-443e-8c30-95e4f2077ca4/resource/f486ea26-7112-423c-9370-08e95bc2669f/download/fsm_t_pop_2025.tif",
        KIR="https://pacificdata.org/data/dataset/2d8164d6-3eb8-4244-94f2-1c0ae31f3358/resource/f43c0771-5dd1-46f3-af78-ab1f43ec3314/download/kir_popgrid_2025.zip",
        MHL="https://pacificdata.org/data/dataset/700b0d93-43ad-46b4-a457-3b82ce491534/resource/7c9a96f4-80ed-4f93-80dc-2738437ff4fa/download/mhl_popgrid_2025.zip",
        NIU="https://pacificdata.org/data/dataset/b4b7a543-5771-47ce-ad78-3c1c73440546/resource/e68f4300-96c0-4b19-a296-d74e9a257c17/download/niu_popgrid_2025.zip",
        NRU="https://pacificdata.org/data/dataset/2c6631ff-78c5-4e2a-9f32-28a64b266ac1/resource/daff6247-4768-4c23-b360-acad66992f4f/download/nru_popgrid_2025.zip",
        PLW="https://pacificdata.org/data/dataset/c36168cf-fb21-410b-81ed-08df086787b0/resource/bbd0e159-940b-4943-82f0-57cda2f91537/download/plw_popgrid_2025.zip",
        SLB="https://pacificdata.org/data/dataset/fd59acdf-5b1f-4113-84e1-0394df21d9f7/resource/61e6c298-9487-4169-8131-4513e82a8e37/download/slb_t_pop_2025.tif",
        TON="https://pacificdata.org/data/dataset/fbe1fdeb-8131-4529-befb-3da1a3c742ef/resource/a144afc4-27ba-47af-bacc-f268f2498254/download/ton_popgrid_2025.zip",
        TUV="https://pacificdata.org/data/dataset/3506e5bd-7afe-413d-a035-32ed5be960c6/resource/b207ed9e-bcae-48a3-83d7-522535fedb86/download/tuv_popgrid_2025.zip",
        VUT="https://pacificdata.org/data/dataset/6a25bbf8-a18d-4f30-9da0-577e1c40db8b/resource/8c5d67bd-afa5-448a-9eec-797d09768bf3/download/vut_t_pop_2025.tif",
        WLF="https://pacificdata.org/data/dataset/9fb68cd6-f83b-440a-b646-7afeb8d76598/resource/b51b4269-c58e-4b19-8b28-f49a5838c12c/download/wlf_t_pop_2025.tif",
        WSM="https://pacificdata.org/data/dataset/bb75be51-0b1d-4fbc-834e-f190b35121dd/resource/3441310e-3bda-4748-b41b-3f03d4b230c2/download/wsm_t_pop_2025.tif",
    )

    if country_code.upper() in worldpop_codes:
        base = "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/2025/"
        suffix = "_pop_2025_CN_100m_R2024B_v1.tif"
        url = f"{base}/{country_code.upper()}/v1/100m/constrained/{country_code.lower()}{suffix}"
        return _open_via_memoryfile(url, country_code)
    elif country_code.upper() in direct_downloads.keys():
        return _open_via_memoryfile(
            direct_downloads[country_code.upper()], country_code
        )


def _open_via_memoryfile(url: str, country_code: str) -> xr.DataArray | xr.Dataset:
    resp = requests.get(url)
    resp.raise_for_status()
    if url.endswith(".zip"):
        with ZipMemoryFile(resp.content) as zipmemfile:
            if country_code == "FJI":
                tif_file = "1_FJI_t_pop_ahs_2023.tif"
            else:
                tif_file = f"{country_code.upper()}_t_pop_2025.tif"
            with zipmemfile.open(tif_file) as tifmemfile:
                return (
                    rx.open_rasterio(tifmemfile, mask_and_scale=True)
                    .squeeze(drop=True)
                    .compute()
                )
    with MemoryFile(resp.content) as memfile:
        return (
            rx.open_rasterio(memfile, mask_and_scale=True).squeeze(drop=True).compute()
        )
