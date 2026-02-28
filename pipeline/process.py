import logging

import numpy as np
import odc.stac
import pystac
import xarray as xr

from pipeline import config

logger = logging.getLogger(__name__)


def load_and_compute(
    items: list[pystac.Item],
    indicator: str,
    year: int,
    month: int,
) -> xr.DataArray | None:
    """STACアイテムをロードし、指標を計算して月次中央値を返す。

    Args:
        items: pystac.Item のリスト（1件以上）
        indicator: "ndvi" | "evi" | "ndwi" | "lst"
        year: 対象年
        month: 対象月

    Returns:
        月次中央値の DataArray（shape: y, x）。有効ピクセルがゼロの場合は None。

    Raises:
        各種処理エラー（呼び出し元の tenacity でリトライ）
    """
    if indicator in ("ndvi", "evi", "ndwi"):
        return _compute_sentinel2(items, indicator, year, month)
    else:
        return _compute_lst(items, year, month)


def _compute_sentinel2(
    items: list[pystac.Item],
    indicator: str,
    year: int,
    month: int,
) -> xr.DataArray | None:
    ds = odc.stac.load(
        items,
        bands=config.SENTINEL2_BANDS,
        bbox=config.BBOX,
        resolution=config.RESOLUTION_S2,
        crs=config.CRS,
        chunks={"x": config.CHUNK_SIZE, "y": config.CHUNK_SIZE},
        groupby="solar_day",
        dtype="uint16",
        nodata=0,
    )

    # SCL マスク：有効クラス以外を NaN 化
    valid = ds.SCL.isin(config.SCL_VALID_CLASSES)

    # スケール変換（uint16 → float32、反射率 0〜1 に正規化）
    red  = ds.red.where(valid).astype("float32") / 10000.0
    nir  = ds.nir.where(valid).astype("float32") / 10000.0
    blue = ds.blue.where(valid).astype("float32") / 10000.0
    swir = ds.swir16.where(valid).astype("float32") / 10000.0

    # 指標計算
    if indicator == "ndvi":
        index_da = (nir - red) / (nir + red)
    elif indicator == "evi":
        index_da = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)
    elif indicator == "ndwi":
        index_da = (nir - swir) / (nir + swir)

    # 月次中央値合成（.compute() で Dask グラフを実体化）
    da = index_da.resample(time="MS").median().compute()
    da = da.isel(time=0)

    return _check_valid(da, indicator, year, month)


def _compute_lst(
    items: list[pystac.Item],
    year: int,
    month: int,
) -> xr.DataArray | None:
    ds = odc.stac.load(
        items,
        bands=config.LANDSAT_BANDS,
        bbox=config.BBOX,
        resolution=config.RESOLUTION_LST,
        crs=config.CRS,
        chunks={"x": config.CHUNK_SIZE, "y": config.CHUNK_SIZE},
        groupby="solar_day",
        dtype="uint16",
        nodata=0,
    )

    # QA_PIXEL マスク：bit 1 = 雲（dilated）、bit 3 = 雲影 をマスク
    cloud_mask = (ds.qa_pixel & 0b0000_1010) == 0

    # nodata=0 のピクセルも除外（lwir11=0 は欠損値）
    lwir = ds.lwir11.where(cloud_mask & (ds.lwir11 != 0)).astype("float32")

    # Kelvin → 摂氏変換
    lst_celsius = lwir * config.LST_SCALE + config.LST_OFFSET - config.LST_KELVIN_OFFSET

    # 月次中央値合成
    da = lst_celsius.resample(time="MS").median().compute()
    da = da.isel(time=0)

    return _check_valid(da, "lst", year, month)


def _check_valid(
    da: xr.DataArray,
    indicator: str,
    year: int,
    month: int,
) -> xr.DataArray | None:
    """有効ピクセル数を確認し、0件の場合は None を返す。"""
    valid_count = int(np.isfinite(da.values).sum())
    total = da.size

    if valid_count == 0:
        logger.warning(
            "[process] %s %d-%02d: no valid pixels, skipping",
            indicator,
            year,
            month,
        )
        return None

    valid_ratio = valid_count / total
    logger.info(
        "[process] %s %d-%02d: computed (valid_ratio=%.1f%%)",
        indicator,
        year,
        month,
        valid_ratio * 100,
    )
    return da
