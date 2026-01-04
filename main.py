import os
import csv
import re
import hashlib
import datetime as dt
import requests
from bs4 import BeautifulSoup
from dateutil import tz

# =========================
# Config
# =========================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]

JST = tz.gettz("Asia/Tokyo")
DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

SOURCES_CSV = "sources.csv"
USER_AGENT = "Mozilla/5.0 (compatible; HokkaidoNewsBot/1.0; +https://github.com/)"

# =========================
# Time window
# =========================
def now_jst() -> dt.datetime:
    return dt.datetime.now(tz=JST)

def window_24h(run_time: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    """
    対象期間：前日 7:00 〜 当日 7:00（JST）
    """
    end = run_time.replace(hour=7, minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=1)
    return start, end

# =========================
# Notion helpers
# =========================
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

def notion_create(title: str, muni: str, link: str, published: dt.datetime | None, fetched: dt.datetime) -> bool:
    key = dup_key(muni, link)
    if notion_exists(key):
        return False

    props = {
        "タイトル": {"title": [{"text": {"content": title[:200]}}]},
        # 自治体は「テキスト（rich_text）」として投入（Selectにしたい場合は別途変更）
        "自治体": {"rich_text": [{"text": {"content": muni}}]},
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

# =========================
# Parsing helpers
# =========================
def parse_jp_date(text: str | None) -> dt.datetime | None:
    """
    'YYYY年M月D日' を JST の 00:00 にして返す
    """
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    return dt.datetime(y, mo, d, 0, 0, 0, tzinfo=JST)

# =========================
# Collector (HTML)
# =========================
def fetch_html(url: str) -> str | None:
    """
    取得に失敗しても例外で止めず None を返す（179自治体化で必須）
    """
    try:
        r = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": USER_AGENT},
        )
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

def collect_html(muni: str, url: str, start: dt.datetime, end: dt.datetime, fetched: dt.datetime) -> int:
    """
    HTMLページ中のリンクを広く走査し、
    直前に現れる 'YYYY年M月D日' を発行日として採用。
    """
    html = fetch_html(url)
    if html is None:
        print(f"[WARN] {muni} fetch_failed url={url}")
        return 0

    soup = BeautifulSoup(html, "html.parser")
    created = 0

    for a in soup.select("a"):
        title = a.get_text(strip=True)
        href = a.get("href")
        if not title or not href:
            continue

        link = requests.compat.urljoin(url, href)

        # 直前の「YYYY年M月D日」らしき文字列を拾う
        prev_date_text = a.find_previous(string=DATE_RE)
        published = parse_jp_date(prev_date_text)

        # 日付が取れないリンクはスキップ（ノイズが多いので）
        if published is None:
            continue

        # 期間フィルタ
        if not (start <= published < end):
            continue

        try:
            if notion_create(title, muni, link, published, fetched):
                created += 1
        except Exception as e:
            # Notion側で落ちても、全体は止めない
            print(f"[WARN] {muni} notion_failed link={link} err={e}")

    return created

# =========================
# Main
# =========================
def read_sources(path: str) -> list[dict]:
    """
    sources.csv:
    muni,url
    北海道庁,https://www.pref.hokkaido.lg.jp/news/
    ...
    """
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # 必須列チェック
    for r in rows:
        if "muni" not in r or "url" not in r or not r["muni"] or not r["url"]:
            raise ValueError("sources.csv must have columns: muni,url and non-empty values")
    return rows

def main():
    fetched = now_jst()
    start, end = window_24h(fetched)

    sources = read_sources(SOURCES_CSV)

    total_created = 0
    for row in sources:
        muni = row["muni"].strip()
        url = row["url"].strip()

        c = collect_html(muni, url, start, end, fetched)
        total_created += c
        print(f"[INFO] {muni} created={c}")

    print(f"[DONE] total_created={total_created}")

if __name__ == "__main__":
    main()
