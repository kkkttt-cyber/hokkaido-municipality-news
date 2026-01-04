import os
import re
import hashlib
import datetime as dt
import requests
from bs4 import BeautifulSoup
from dateutil import tz

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]

JST = tz.gettz("Asia/Tokyo")

NEWS_URL = "https://www.pref.hokkaido.lg.jp/news/"

DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

def now_jst():
    return dt.datetime.now(tz=JST)

def window_24h(run_time: dt.datetime):
    end = run_time.replace(hour=7, minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=1)
    return start, end

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

def dup_key(muni: str, url: str) -> str:
    return hashlib.sha256(f"{muni}|{url}".encode("utf-8")).hexdigest()

def notion_exists(key: str) -> bool:
    r = requests.post(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        headers=notion_headers(),
        json={"filter": {"property": "重複キー", "rich_text": {"equals": key}}},
        timeout=30,
    )
    r.raise_for_status()
    return len(r.json().get("results", [])) > 0

def notion_create(title: str, muni: str, link: str, published: dt.datetime | None, fetched: dt.datetime):
    key = dup_key(muni, link)
    if notion_exists(key):
        return False

    props = {
        "タイトル": {"title": [{"text": {"content": title[:200]}}]},
        "自治体": {"select": {"name": muni}},
        "URL": {"url": link},
        "取得日時": {"date": {"start": fetched.isoformat()}},
        "重複キー": {"rich_text": [{"text": {"content": key}}]},
    }
    if published is not None:
        props["発行日"] = {"date": {"start": published.isoformat()}}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(),
        json={"parent": {"database_id": DB_ID}, "properties": props},
        timeout=30,
    )
    r.raise_for_status()
    return True

def parse_jp_date(s: str) -> dt.datetime | None:
    m = DATE_RE.search(s)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    # 日付しかないので JST の 00:00 扱い
    return dt.datetime(y, mo, d, 0, 0, 0, tzinfo=JST)

def collect_hokkaido_news():
    r = requests.get(NEWS_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    items = []
    # このページは「日付テキスト」→「h3のリンク」…の並びになっている
    for a in soup.select("h3 a"):
        title = a.get_text(strip=True)
        href = a.get("href", "").strip()
        if not href:
            continue
        link = requests.compat.urljoin(NEWS_URL, href)

        # 直前に出てくる「YYYY年M月D日」を探す
        prev_date_text = a.find_parent("h3").find_previous(string=DATE_RE)
        published = parse_jp_date(prev_date_text) if prev_date_text else None

        items.append((title, link, published))
    return items

def main():
    fetched = now_jst()
    start, end = window_24h(fetched)

    created = 0
    muni = "北海道庁"

    for title, link, published in collect_hokkaido_news():
        # 日付が取れないものは一旦スキップ（必要なら後で「取得日時で扱う」に変更可）
        if published is None:
            continue

        # 24時間窓：前日7:00〜当日7:00
        if start <= published < end:
            if notion_create(title, muni, link, published, fetched):
                created += 1

    print(f"Created pages: {created}")

if __name__ == "__main__":
    main()
