import logging
import pathlib

import numpy as np
import pandas as pd
import rioxarray  # noqa: F401 - .rio アクセサを登録
import xarray as xr
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from pipeline import config

logger = logging.getLogger(__name__)


def save_cog(
    da: xr.DataArray,
    indicator: str,
    year: int,
    month: int,
) -> pathlib.Path:
    """DataArray を Cloud Optimized GeoTIFF として保存する。

    Args:
        da: 月次中央値 DataArray（shape: y, x）
        indicator: 指標名
        year: 対象年
        month: 対象月

    Returns:
        保存した COG のパス

    Raises:
        各種 I/O エラー（呼び出し元で欠損記録）
    """
    out_dir = pathlib.Path(config.OUTPUT_DIR) / indicator
    out_dir.mkdir(parents=True, exist_ok=True)

    output_path = out_dir / f"{indicator}_{year:04d}_{month:02d}.tif"
    tmp_path = out_dir / f"_tmp_{indicator}_{year:04d}_{month:02d}.tif"

    # CRS が未設定の場合は明示的に書き込む
    if da.rio.crs is None:
        da = da.rio.write_crs(config.CRS)

    # 空間次元を明示（odc-stac は "x"/"y" を使用）
    da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")

    try:
        da.rio.to_raster(str(tmp_path), dtype="float32")
        cog_translate(
            str(tmp_path),
            str(output_path),
            cog_profiles.get("deflate"),
            in_memory=False,
            quiet=True,
        )
    finally:
        # 成功・失敗にかかわらず一時ファイルを削除
        if tmp_path.exists():
            tmp_path.unlink()

    size_kb = output_path.stat().st_size / 1024
    logger.info("[export] saved %s (%.0f KB)", output_path, size_kb)
    return output_path


def update_summary(
    da: xr.DataArray,
    indicator: str,
    year: int,
    month: int,
) -> None:
    """時系列サマリー CSV/JSON を更新（upsert）する。

    Args:
        da: 月次中央値 DataArray（shape: y, x）
        indicator: 指標名
        year: 対象年
        month: 対象月
    """
    csv_path = pathlib.Path(config.OUTPUT_DIR) / f"summary_{indicator}.csv"
    json_path = pathlib.Path(config.OUTPUT_DIR) / f"summary_{indicator}.json"

    # 出力ディレクトリを作成
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # 統計値を計算
    values = da.values.astype("float64")
    total_pixels = values.size
    valid_pixels = int(np.isfinite(values).sum())

    new_row = {
        "year": int(year),
        "month": int(month),
        "mean": float(np.nanmean(values)),
        "max": float(np.nanmax(values)),
        "min": float(np.nanmin(values)),
        "valid_ratio": float(valid_pixels / total_pixels) if total_pixels > 0 else 0.0,
    }

    # 既存 CSV を読み込み upsert する（なければ新規行から開始）
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        # 同年月の既存行を削除してから追加
        df = df[~((df["year"] == year) & (df["month"] == month))]
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df = pd.DataFrame([new_row])
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    # 整数型を維持
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    logger.info("[export] summary updated: %s %d-%02d", indicator, year, month)
