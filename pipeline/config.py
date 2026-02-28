import os
from datetime import date

# ── エリア定義 ──────────────────────────────────────────
BBOX = [139.543, 35.594, 139.582, 35.626]  # [west, south, east, north]
CRS = "EPSG:32654"                          # UTM Zone 54N（関東）

# ── 時間軸 ──────────────────────────────────────────────
START_YEAR  = 2016
START_MONTH = 1
# END は実行時に引数で上書き可能。デフォルトは当月
END_YEAR    = date.today().year
END_MONTH   = date.today().month

# ── STAC / odc-stac ─────────────────────────────────────
STAC_URL        = "https://planetarycomputer.microsoft.com/api/stac/v1"
CLOUD_COVER_MAX = 20   # eo:cloud_cover フィルタ上限（%）
CHUNK_SIZE      = 2048  # {"x": CHUNK_SIZE, "y": CHUNK_SIZE}
RESOLUTION_S2   = 10   # Sentinel-2 系（NDVI/EVI/NDWI）
RESOLUTION_LST  = 30   # Landsat 8/9 （LST）

# ── 指標定義 ─────────────────────────────────────────────
INDICATORS = ["ndvi", "evi", "ndwi", "lst"]

SENTINEL2_BANDS = ["red", "nir", "blue", "swir16", "SCL"]
LANDSAT_BANDS   = ["lwir11", "qa_pixel"]

# SCL 有効クラス（植生=4, 裸地=5, 水=6, 未分類=7）
SCL_VALID_CLASSES = [4, 5, 6, 7]

# ── Landsat LST 変換係数（ST_B10 → 摂氏） ───────────────
LST_SCALE  = 0.00341802
LST_OFFSET = 149.0
LST_KELVIN_OFFSET = 273.15

# ── 出力ディレクトリ ─────────────────────────────────────
OUTPUT_DIR  = os.environ.get("OUTPUT_DIR", "output")
MISSING_LOG = os.path.join(OUTPUT_DIR, "missing.json")

# ── GitHub ───────────────────────────────────────────────
GITHUB_REPO   = os.environ.get("GITHUB_REPO", "")    # "owner/repo" 形式
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")

# ── リトライ設定 ─────────────────────────────────────────
RETRY_ATTEMPTS   = 3
RETRY_WAIT_MIN   = 10   # 秒
RETRY_WAIT_MAX   = 60   # 秒
