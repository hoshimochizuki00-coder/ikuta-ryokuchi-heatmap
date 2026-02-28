"""
tests/test_export.py

pipeline/export.py のユニットテスト。
tmp_path フィクスチャで隔離されたファイル I/O を使用する。
cog_translate は mock でバイパスし、rio への依存は最小限にする。

検証項目:
  - save_cog: 出力パス構造、一時ファイルの削除
  - update_summary: CSV/JSON 作成、統計値の正確性、upsert、ソート順
"""

import json
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from unittest.mock import patch


# ── ヘルパー関数 ────────────────────────────────────────────────────────────────

def _make_da(values: list[list[float]]) -> xr.DataArray:
    """空間座標付きの 2D DataArray を作成する。"""
    arr = np.array(values, dtype="float32")
    x = np.arange(arr.shape[1], dtype="float64") * 10.0
    y = np.arange(arr.shape[0], dtype="float64") * -10.0
    return xr.DataArray(arr, dims=["y", "x"], coords={"x": x, "y": y})


def _make_spatial_da(values: list[list[float]]) -> xr.DataArray:
    """CRS 付きの 2D DataArray を作成する（save_cog 用）。"""
    import rioxarray  # noqa: F401
    arr = np.array(values, dtype="float32")
    # UTM Zone 54N の適当な座標
    x = np.array([500000.0 + i * 10.0 for i in range(arr.shape[1])])
    y = np.array([3944000.0 - i * 10.0 for i in range(arr.shape[0])])
    da = xr.DataArray(arr, dims=["y", "x"], coords={"x": x, "y": y})
    da = da.rio.write_crs("EPSG:32654")
    return da


# ── save_cog ────────────────────────────────────────────────────────────────────

class TestSaveCog:
    def test_output_path_structure(self, tmp_path, monkeypatch):
        """出力パスが {OUTPUT_DIR}/{indicator}/{indicator}_{YYYY}_{MM}.tif になること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_spatial_da([[0.5, 0.6], [0.7, 0.8]])

        def fake_cog_translate(src, dst, *args, **kwargs):
            pathlib.Path(dst).touch()

        with patch("pipeline.export.cog_translate", side_effect=fake_cog_translate):
            from pipeline.export import save_cog
            result = save_cog(da, "ndvi", 2023, 7)

        expected = tmp_path / "ndvi" / "ndvi_2023_07.tif"
        assert result == expected
        assert expected.exists()

    def test_tmp_file_cleaned_up_on_success(self, tmp_path, monkeypatch):
        """cog_translate 成功後に一時ファイルが削除されること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_spatial_da([[0.3, 0.4]])
        captured_tmp: dict = {}

        original_to_raster = None

        def fake_cog_translate(src, dst, *args, **kwargs):
            captured_tmp["tmp"] = pathlib.Path(src)
            pathlib.Path(dst).touch()

        with patch("pipeline.export.cog_translate", side_effect=fake_cog_translate):
            from pipeline.export import save_cog
            save_cog(da, "ndvi", 2023, 7)

        if "tmp" in captured_tmp:
            assert not captured_tmp["tmp"].exists(), "一時ファイルが削除されていない"

    def test_output_directory_created(self, tmp_path, monkeypatch):
        """指標ディレクトリが存在しなくても自動作成されること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_spatial_da([[0.5, 0.6]])

        def fake_cog_translate(src, dst, *args, **kwargs):
            pathlib.Path(dst).touch()

        with patch("pipeline.export.cog_translate", side_effect=fake_cog_translate):
            from pipeline.export import save_cog
            save_cog(da, "lst", 2023, 7)

        assert (tmp_path / "lst").is_dir()


# ── update_summary ──────────────────────────────────────────────────────────────

class TestUpdateSummary:
    def test_csv_created_with_correct_schema(self, tmp_path, monkeypatch):
        """初回呼び出しで正しいカラム順の CSV が作成されること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_da([[0.5, 0.6], [0.7, 0.8]])
        from pipeline.export import update_summary
        update_summary(da, "ndvi", 2023, 7)

        csv_path = tmp_path / "summary_ndvi.csv"
        assert csv_path.exists()
        df = pd.read_csv(csv_path)
        assert list(df.columns) == ["year", "month", "mean", "max", "min", "valid_ratio"]
        assert len(df) == 1
        assert df.iloc[0]["year"] == 2023
        assert df.iloc[0]["month"] == 7

    def test_statistics_correct(self, tmp_path, monkeypatch):
        """mean / max / min / valid_ratio が正確に計算されること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        values = [[0.2, 0.4], [0.6, 0.8]]
        da = _make_da(values)
        from pipeline.export import update_summary
        update_summary(da, "ndwi", 2023, 7)

        df = pd.read_csv(tmp_path / "summary_ndwi.csv")
        row = df.iloc[0]
        flat = np.array(values, dtype="float64").ravel()
        assert pytest.approx(row["mean"], rel=1e-4) == float(np.mean(flat))
        assert pytest.approx(row["max"],  rel=1e-4) == float(np.max(flat))
        assert pytest.approx(row["min"],  rel=1e-4) == float(np.min(flat))
        assert pytest.approx(row["valid_ratio"], rel=1e-4) == 1.0

    def test_valid_ratio_excludes_nan(self, tmp_path, monkeypatch):
        """NaN ピクセルが valid_ratio から除外されること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        values = np.array([[0.5, float("nan")], [0.7, 0.8]], dtype="float32")
        x = np.array([0.0, 10.0])
        y = np.array([0.0, -10.0])
        da = xr.DataArray(values, dims=["y", "x"], coords={"x": x, "y": y})

        from pipeline.export import update_summary
        update_summary(da, "ndvi", 2023, 8)

        df = pd.read_csv(tmp_path / "summary_ndvi.csv")
        # 4ピクセル中3ピクセルが有効 → 0.75
        assert pytest.approx(df.iloc[0]["valid_ratio"], rel=1e-4) == 0.75

    def test_upsert_no_duplicate_rows(self, tmp_path, monkeypatch):
        """同年月を 2 回書き込んでも行数が増えないこと。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da1 = _make_da([[0.3, 0.4]])
        da2 = _make_da([[0.8, 0.9]])
        from pipeline.export import update_summary
        update_summary(da1, "evi", 2023, 7)
        update_summary(da2, "evi", 2023, 7)

        df = pd.read_csv(tmp_path / "summary_evi.csv")
        assert len(df) == 1, "upsert で行が重複した"
        expected_mean = float(np.mean([0.8, 0.9]))
        assert pytest.approx(df.iloc[0]["mean"], rel=1e-4) == expected_mean

    def test_sort_order_year_month_asc(self, tmp_path, monkeypatch):
        """year 昇順、month 昇順でソートされること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_da([[0.5, 0.6]])
        from pipeline.export import update_summary
        update_summary(da, "lst", 2023, 12)
        update_summary(da, "lst", 2023, 1)
        update_summary(da, "lst", 2022, 6)

        df = pd.read_csv(tmp_path / "summary_lst.csv")
        assert list(df["year"])  == [2022, 2023, 2023]
        assert list(df["month"]) == [6, 1, 12]

    def test_json_matches_csv(self, tmp_path, monkeypatch):
        """JSON の内容が CSV と一致すること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_da([[0.6, 0.7]])
        from pipeline.export import update_summary
        update_summary(da, "ndvi", 2023, 7)

        json_path = tmp_path / "summary_ndvi.json"
        assert json_path.exists()
        with open(json_path, encoding="utf-8") as f:
            records = json.load(f)
        assert isinstance(records, list)
        assert len(records) == 1
        assert records[0]["year"] == 2023
        assert records[0]["month"] == 7
        assert "mean" in records[0]
        assert "valid_ratio" in records[0]

    def test_multiple_months_accumulated(self, tmp_path, monkeypatch):
        """複数月を書き込んで行数が正しく蓄積されること。"""
        import pipeline.config as cfg
        monkeypatch.setattr(cfg, "OUTPUT_DIR", str(tmp_path))

        da = _make_da([[0.5]])
        from pipeline.export import update_summary
        for month in [1, 2, 3]:
            update_summary(da, "ndvi", 2023, month)

        df = pd.read_csv(tmp_path / "summary_ndvi.csv")
        assert len(df) == 3
        assert list(df["month"]) == [1, 2, 3]
