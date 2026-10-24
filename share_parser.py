"""番茄小说分享链接解析。

只负责从聊天文本里识别出 book_id 或短链；
短链的真实 ID 交给后端跟随跳转（后端有域名白名单校验）。
"""

import re
from typing import Optional, Tuple

RE_DIGITS = re.compile(r"^\d{10,25}$")
RE_QS = re.compile(r"[?&]book_id=(\d{10,25})")
RE_PAGE = re.compile(r"/page/(\d{10,25})")
RE_URL = re.compile(r"https?://[^\s\u4e00-\u9fff\"'<>]+")
RE_SHORT = re.compile(r"^https?://(?:[a-z0-9-]+\.)?(?:changdunovel|fanqienovel)\.com/t/")


def extract_url(text: str) -> Optional[str]:
    """从带说明文字的分享内容里取出第一个 URL。"""
    if not text:
        return None
    m = RE_URL.search(text.strip())
    return m.group(0).rstrip("，。、；") if m else None


def parse_book_id(text: str) -> Tuple[Optional[str], bool]:
    """解析出 book_id。

    返回 (book_id, need_resolve)：
    need_resolve=True 表示拿到的是短链，需要后端跟随跳转才能得到 ID。
    """
    if not text:
        return None, False
    raw = text.strip()

    if RE_DIGITS.match(raw):
        return raw, False

    url = extract_url(raw) or raw

    m = RE_QS.search(url)
    if m:
        return m.group(1), False

    m = RE_PAGE.search(url)
    if m:
        return m.group(1), False

    if RE_SHORT.match(url):
        return None, True

    return None, False


def looks_like_fanqie(text: str) -> bool:
    """粗判这条消息是否包含番茄链接，用于自动触发。"""
    url = extract_url(text or "")
    if not url:
        return False
    return any(h in url for h in ("changdunovel.com", "fanqienovel.com"))
