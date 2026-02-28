import logging
import pathlib
import subprocess

from pipeline import config

logger = logging.getLogger(__name__)


def _run_gh(args: list[str]) -> subprocess.CompletedProcess:
    """gh CLI を実行し、失敗時は RuntimeError を送出する。"""
    cmd = ["gh"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"gh {' '.join(args)} failed: {result.stderr.strip()}"
        )
    return result


def ensure_release(tag: str, title: str) -> None:
    """指定タグの GitHub Release が存在しなければ作成する。

    Args:
        tag: リリースタグ名（例: "data-ndvi-2023"）
        title: リリースタイトル（例: "NDVI 2023"）
    """
    if not config.GITHUB_REPO:
        logger.warning("[upload] GITHUB_REPO not set, skipping ensure_release(%s)", tag)
        return
    try:
        _run_gh(["release", "view", tag, "--repo", config.GITHUB_REPO])
    except RuntimeError:
        # Release does not exist; create it
        _run_gh([
            "release", "create", tag,
            "--title", title,
            "--notes", "",
            "--repo", config.GITHUB_REPO,
        ])


def upload_asset(tag: str, file_path: pathlib.Path) -> None:
    """GitHub Release にアセットをアップロードする（同名アセットは事前削除）。

    Args:
        tag: リリースタグ名
        file_path: アップロードするファイルのパス

    Raises:
        RuntimeError: アップロードが失敗した場合
    """
    if not config.GITHUB_REPO:
        logger.warning(
            "[upload] GITHUB_REPO not set, skipping upload_asset(%s)", file_path.name
        )
        return
    filename = file_path.name

    # 同名アセットが既に存在する場合は削除（上書き対応）
    try:
        _run_gh([
            "release", "delete-asset", tag, filename,
            "--yes", "--repo", config.GITHUB_REPO,
        ])
    except RuntimeError:
        pass  # アセットが存在しない場合は無視

    try:
        _run_gh([
            "release", "upload", tag, str(file_path),
            "--repo", config.GITHUB_REPO,
        ])
        logger.info("[upload] %s -> %s", filename, tag)
    except RuntimeError as exc:
        logger.error("[upload] failed %s: %s", filename, exc)
        raise


def upload_summary(indicator: str) -> None:
    """data-summary タグに summary CSV/JSON をアップロードする。

    Args:
        indicator: 指標名（"ndvi", "evi", "ndwi", "lst"）
    """
    if not config.GITHUB_REPO:
        logger.warning(
            "[upload] GITHUB_REPO not set, skipping upload_summary(%s)", indicator
        )
        return
    tag = "data-summary"
    ensure_release(tag, "Summary Data")
    for ext in ("csv", "json"):
        file_path = pathlib.Path(config.OUTPUT_DIR) / f"summary_{indicator}.{ext}"
        if file_path.exists():
            upload_asset(tag, file_path)
