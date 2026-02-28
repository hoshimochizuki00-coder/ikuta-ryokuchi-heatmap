"""
生田緑地 衛星データ処理パイプライン エントリーポイント

使用方法:
    python pipeline/main.py --mode historical --start 2023-07 --end 2023-07
    python pipeline/main.py --mode monthly
"""

import pathlib
import sys

# `python pipeline/main.py` として実行した場合にプロジェクトルートを sys.path に追加
_project_root = pathlib.Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import argparse
import json
import logging
import pathlib
import sys
from datetime import date

import xarray as xr
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_random

from pipeline import config
from pipeline import export, process, query, upload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ジョブ実行中に蓄積する欠損レコード（ジョブ開始時にリセット）
_missing_records: list[dict] = []


def month_range(
    start: tuple[int, int],
    end: tuple[int, int],
):
    """start から end まで（両端を含む）の (year, month) タプルを順に yield する。"""
    y, m = start
    ey, em = end
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def record_missing(
    year: int,
    month: int,
    indicator: str,
    reason: str = "unknown",
) -> None:
    """欠損を _missing_records に追記し、output/missing.json を上書き保存する。

    reason の種類:
        no_items        STACアイテムが0件
        no_valid_pixels マスク後に有効ピクセルなし
        process_error   計算中に予期しないエラー
        upload_error    アップロード失敗
    """
    _missing_records.append({
        "year": year,
        "month": month,
        "indicator": indicator,
        "reason": reason,
    })
    out_path = pathlib.Path(config.MISSING_LOG)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(_missing_records, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def process_one_month(
    year: int,
    month: int,
    indicator: str,
) -> xr.DataArray | None:
    """1ヶ月分・1指標のデータを取得・計算する。tenacity でリトライ付き。

    Returns:
        成功時は DataArray（shape: y, x）、失敗時は None
    """
    collection = "landsat-c2-l2" if indicator == "lst" else "sentinel-2-l2a"

    @retry(
        stop=stop_after_attempt(config.RETRY_ATTEMPTS),
        wait=wait_random(min=config.RETRY_WAIT_MIN, max=config.RETRY_WAIT_MAX),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _inner() -> xr.DataArray | None:
        items = query.search_items(collection, year, month)
        if not items:
            return None
        return process.load_and_compute(items, indicator, year, month)

    try:
        return _inner()
    except Exception as exc:
        logger.error(
            "[main] %s %d-%02d: all retries failed: %s",
            indicator,
            year,
            month,
            exc,
        )
        return None


def _parse_args() -> argparse.Namespace:
    today = date.today()
    default_ym = f"{today.year}-{today.month:02d}"
    parser = argparse.ArgumentParser(
        description="生田緑地 衛星データ処理パイプライン",
    )
    parser.add_argument(
        "--mode",
        choices=["historical", "monthly"],
        required=True,
        help="実行モード",
    )
    parser.add_argument(
        "--start",
        default=default_ym,
        help="処理開始月 (YYYY-MM)。省略時は当月",
    )
    parser.add_argument(
        "--end",
        default=default_ym,
        help="処理終了月 (YYYY-MM)。省略時は当月",
    )
    return parser.parse_args()


def _parse_ym(s: str) -> tuple[int, int]:
    parts = s.split("-")
    return int(parts[0]), int(parts[1])


def main() -> None:
    args = _parse_args()
    start = _parse_ym(args.start)
    end = _parse_ym(args.end)

    months = list(month_range(start, end))
    success_count = 0
    missing_count = 0

    for year, month in months:
        for indicator in config.INDICATORS:
            result = process_one_month(year, month, indicator)

            if result is None:
                record_missing(year, month, indicator, "no_items")
                missing_count += 1
                continue

            # COG 書き出し + サマリー更新
            try:
                cog_path = export.save_cog(result, indicator, year, month)
                export.update_summary(result, indicator, year, month)
            except Exception as exc:
                logger.error(
                    "[main] export failed %s %d-%02d: %s",
                    indicator,
                    year,
                    month,
                    exc,
                )
                record_missing(year, month, indicator, "process_error")
                missing_count += 1
                continue

            # GitHub Releases アップロード（GITHUB_REPO が設定されている場合のみ）
            if config.GITHUB_REPO:
                try:
                    tag = f"data-{indicator}-{year}"
                    title = f"{indicator.upper()} {year}"
                    upload.ensure_release(tag, title)
                    upload.upload_asset(tag, cog_path)
                except Exception as exc:
                    logger.error(
                        "[main] upload failed %s %d-%02d: %s",
                        indicator,
                        year,
                        month,
                        exc,
                    )
                    record_missing(year, month, indicator, "upload_error")
                    missing_count += 1
                    continue

            success_count += 1

        # 月ループ終了後にサマリーをアップロード
        if config.GITHUB_REPO:
            for indicator in config.INDICATORS:
                upload.upload_summary(indicator)

    # 欠損ファイルが未作成（欠損 0 件）の場合も空配列で作成する
    out_path = pathlib.Path(config.MISSING_LOG)
    if not out_path.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("[]", encoding="utf-8")

    # 終了サマリーログ
    logger.info("[main] ===== 処理完了 =====")
    logger.info("[main] 処理月数：%d", len(months))
    logger.info("[main] 成功：%d", success_count)
    if missing_count > 0:
        logger.warning("[main] 欠損：%d  → %s を参照", missing_count, config.MISSING_LOG)
    else:
        logger.info("[main] 欠損：0")

    # 欠損があってもexit(0)。ワークフローを失敗扱いにしない。
    sys.exit(0)


if __name__ == "__main__":
    main()
