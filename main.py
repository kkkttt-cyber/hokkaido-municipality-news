import os
import csv
import re
import hashlib
import datetime as dt
import requests
from bs4 import BeautifulSoup
from dateutil import tz

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]

JST = tz.gettz("Asia/Tokyo")
DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

def now_jst():
    return dt.datetime.now(tz=JST)

def window_24h(now):
    end = now.replace(hour=7, minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=1)
    return start, end

def headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

def dup_key(muni, url):
    return hashlib.sha256(f"{muni}|{url}".encode()).hexdigest()

def exists(key):
    r = requests.post(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        headers=headers(),
        json={"filter": {"property": "重複キー", "rich_text": {"equals": key}}},
        timeout=30,
    )
    return len(r.json().get("results", [])) > 0

def create(title, muni, link, published, fetched):
    key = dup_key(muni, link)
    if exists(key):
        return

    props = {
        "タイトル": {"title": [{"text": {"content": title[:200]}}]},
        "自治体": {"rich_text": [{"text": {"content": muni}}]},
        "URL": {"url": link},
        "取得日時": {"date": {"start": fetched.isoformat()}},
        "重複キー": {"rich_text": [{"text": {"content": key}}]},
    }
    if published:
        props["発行日"] = {"date": {"start": published.isoformat()}}

    requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers(),
        json={"parent": {"database_id": DB_ID}, "properties": props},
        timeout=30,
    )

def parse_date(text):
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    y, mth, d = map(int, m.groups())
    return dt.datetime(y, mth, d, tzinfo=JST)

def collect_html(muni, url, start, end, fetched):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.select("a"):
        title = a.get_text(strip=True)
        href = a.get("href")
        if not title or not href:
            continue

        link = requests.compat.urljoin(url, href)
        date_text = a.find_previous(string=DATE_RE)
        published = parse_date(date_text)

        if published and start <= published < end:
            create(title, muni, link, published, fetched)

def main():
    fetched = now_jst()
    start, end = window_24h(fetched)

    with open("sources.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            collect_html(row["muni"], row["url"], start, end, fetched)

if __name__ == "__main__":
    main()
