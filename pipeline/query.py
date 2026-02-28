import calendar
import logging

import planetary_computer
import pystac
import pystac_client

from pipeline import config

logger = logging.getLogger(__name__)


def search_items(collection: str, year: int, month: int) -> list[pystac.Item]:
    """STAC API を検索してアイテムリストを返す。

    Args:
        collection: "sentinel-2-l2a" または "landsat-c2-l2"
        year: 対象年
        month: 対象月

    Returns:
        pystac.Item のリスト（0件の場合は空リスト）

    Raises:
        各種ネットワーク・API エラー（呼び出し元の tenacity でリトライ）
    """
    last_day = calendar.monthrange(year, month)[1]
    datetime_range = f"{year}-{month:02d}-01/{year}-{month:02d}-{last_day:02d}"

    catalog = pystac_client.Client.open(
        config.STAC_URL,
        modifier=planetary_computer.sign_inplace,
    )
    search = catalog.search(
        collections=[collection],
        bbox=config.BBOX,
        datetime=datetime_range,
        query={"eo:cloud_cover": {"lt": config.CLOUD_COVER_MAX}},
    )
    items = list(search.items())

    level = logging.WARNING if not items else logging.INFO
    logger.log(
        level,
        "[query] %s %d-%02d: %d items found",
        collection,
        year,
        month,
        len(items),
    )
    return items
