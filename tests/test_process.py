"""
tests/test_process.py

pipeline/process.py のユニットテスト。
odc.stac.load を unittest.mock でモックし、ネットワーク不要で実行できる。

検証項目:
  - NDVI / EVI / NDWI / LST の計算式の正確性
  - SCL マスク（無効クラスで全 NaN → None 返却）
  - QA_PIXEL マスク（bit 1 / bit 3 セットで全 NaN → None 返却）
  - LST の nodata=0 除外
  - .isel(time=0) 後の出力 shape が (y, x) であること
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from unittest.mock import patch


# ── ヘルパー関数 ────────────────────────────────────────────────────────────────

def _make_s2_dataset(scl_value: int, band_values: dict) -> xr.Dataset:
    """2x2 Sentinel-2 Dataset を 1 タイムステップで作成する。"""
    time = pd.to_datetime(["2023-07-15"])
    x = np.array([0.0, 10.0])
    y = np.array([0.0, -10.0])
    coords = {"time": time, "x": x, "y": y}
    shape = (1, 2, 2)

    data_vars = {}
    for band, value in band_values.items():
        data = np.full(shape, value, dtype="uint16")
        data_vars[band] = xr.DataArray(data, dims=["time", "y", "x"], coords=coords)

    scl_data = np.full(shape, scl_value, dtype="uint16")
    data_vars["SCL"] = xr.DataArray(scl_data, dims=["time", "y", "x"], coords=coords)

    return xr.Dataset(data_vars)


def _make_landsat_dataset(qa_value: int, lwir_value: int) -> xr.Dataset:
    """2x2 Landsat Dataset を 1 タイムステップで作成する。"""
    time = pd.to_datetime(["2023-07-15"])
    x = np.array([0.0, 30.0])
    y = np.array([0.0, -30.0])
    coords = {"time": time, "x": x, "y": y}
    shape = (1, 2, 2)

    qa_data = np.full(shape, qa_value, dtype="uint16")
    lwir_data = np.full(shape, lwir_value, dtype="uint16")

    return xr.Dataset({
        "qa_pixel": xr.DataArray(qa_data, dims=["time", "y", "x"], coords=coords),
        "lwir11": xr.DataArray(lwir_data, dims=["time", "y", "x"], coords=coords),
    })


# ── NDVI ────────────────────────────────────────────────────────────────────────

class TestNDVI:
    def test_ndvi_correct_formula(self):
        """NDVI = (nir - red) / (nir + red)。既知値で計算式を確認する。"""
        ds = _make_s2_dataset(scl_value=4, band_values={
            "red": 4000, "nir": 8000, "blue": 2000, "swir16": 1000,
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "ndvi", 2023, 7)

        assert result is not None
        expected = (0.8 - 0.4) / (0.8 + 0.4)
        np.testing.assert_allclose(result.values, expected, rtol=1e-4)

    def test_ndvi_output_shape_is_2d(self):
        """time 次元がスクイーズされ、出力が (y, x) の 2D DataArray になること。"""
        ds = _make_s2_dataset(scl_value=4, band_values={
            "red": 3000, "nir": 7000, "blue": 1500, "swir16": 1000,
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "ndvi", 2023, 7)

        assert result is not None
        assert result.ndim == 2, f"Expected 2D, got {result.ndim}D with dims {result.dims}"
        assert "time" not in result.dims
        assert result.shape == (2, 2)

    def test_ndvi_scl_invalid_class_returns_none(self):
        """SCL クラス 3（雲影）は無効クラスのため、全ピクセルがマスクされ None を返す。"""
        ds = _make_s2_dataset(scl_value=3, band_values={
            "red": 4000, "nir": 8000, "blue": 2000, "swir16": 1000,
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "ndvi", 2023, 7)

        assert result is None

    def test_ndvi_scl_class5_is_valid(self):
        """SCL クラス 5（裸地）は有効クラスのため、NaN にならない。"""
        ds = _make_s2_dataset(scl_value=5, band_values={
            "red": 2000, "nir": 6000, "blue": 1000, "swir16": 500,
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "ndvi", 2023, 7)

        assert result is not None
        assert not np.all(np.isnan(result.values))

    def test_ndvi_scl_class6_is_valid(self):
        """SCL クラス 6（水域）は有効クラスのため、NaN にならない。"""
        ds = _make_s2_dataset(scl_value=6, band_values={
            "red": 1000, "nir": 2000, "blue": 3000, "swir16": 500,
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "ndvi", 2023, 7)

        assert result is not None


# ── EVI ────────────────────────────────────────────────────────────────────────

class TestEVI:
    def test_evi_correct_formula(self):
        """EVI = 2.5 * (nir - red) / (nir + 6*red - 7.5*blue + 1)。"""
        nir_r, red_r, blue_r = 0.8, 0.4, 0.2
        expected = 2.5 * (nir_r - red_r) / (nir_r + 6 * red_r - 7.5 * blue_r + 1.0)

        ds = _make_s2_dataset(scl_value=4, band_values={
            "red":    int(red_r  * 10000),
            "nir":    int(nir_r  * 10000),
            "blue":   int(blue_r * 10000),
            "swir16": 1000,
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "evi", 2023, 7)

        assert result is not None
        np.testing.assert_allclose(result.values, expected, rtol=1e-3)


# ── NDWI ───────────────────────────────────────────────────────────────────────

class TestNDWI:
    def test_ndwi_correct_formula(self):
        """NDWI = (nir - swir16) / (nir + swir16)。"""
        nir_r, swir_r = 0.6, 0.2
        expected = (nir_r - swir_r) / (nir_r + swir_r)

        ds = _make_s2_dataset(scl_value=6, band_values={
            "red":    3000,
            "nir":    int(nir_r  * 10000),
            "blue":   1000,
            "swir16": int(swir_r * 10000),
        })
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "ndwi", 2023, 7)

        assert result is not None
        np.testing.assert_allclose(result.values, expected, rtol=1e-3)


# ── LST ────────────────────────────────────────────────────────────────────────

class TestLST:
    def test_lst_celsius_formula(self):
        """LST [°C] = lwir11 * 0.00341802 + 149.0 - 273.15。"""
        lwir_dn = 20000
        expected_celsius = lwir_dn * 0.00341802 + 149.0 - 273.15

        ds = _make_landsat_dataset(qa_value=0, lwir_value=lwir_dn)
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "lst", 2023, 7)

        assert result is not None
        np.testing.assert_allclose(result.values, expected_celsius, rtol=1e-4)

    def test_lst_qa_cloud_bit1_returns_none(self):
        """QA_PIXEL bit 1（dilated cloud）がセットされていると全ピクセルがマスクされ None を返す。"""
        # bit 1 = 0b00000010 = 2
        ds = _make_landsat_dataset(qa_value=0b0000_0010, lwir_value=20000)
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "lst", 2023, 7)

        assert result is None

    def test_lst_qa_shadow_bit3_returns_none(self):
        """QA_PIXEL bit 3（cloud shadow）がセットされていると全ピクセルがマスクされ None を返す。"""
        # bit 3 = 0b00001000 = 8
        ds = _make_landsat_dataset(qa_value=0b0000_1000, lwir_value=20000)
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "lst", 2023, 7)

        assert result is None

    def test_lst_qa_clear_returns_valid(self):
        """QA_PIXEL = 0（全クリア）の場合は有効な結果を返す。"""
        ds = _make_landsat_dataset(qa_value=0, lwir_value=15000)
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "lst", 2023, 7)

        assert result is not None
        assert result.shape == (2, 2)

    def test_lst_nodata_zero_returns_none(self):
        """lwir11 = 0 は nodata 値のため、qa=0 でもマスクされて None を返す。"""
        ds = _make_landsat_dataset(qa_value=0, lwir_value=0)
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "lst", 2023, 7)

        assert result is None

    def test_lst_output_shape_is_2d(self):
        """time 次元がスクイーズされ、出力が (y, x) の 2D DataArray になること。"""
        ds = _make_landsat_dataset(qa_value=0, lwir_value=30000)
        with patch("odc.stac.load", return_value=ds):
            from pipeline.process import load_and_compute
            result = load_and_compute([], "lst", 2023, 7)

        assert result is not None
        assert result.ndim == 2
        assert "time" not in result.dims
