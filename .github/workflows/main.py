import os
import hashlib
import datetime as dt
import requests
import feedparser
from dateutil import tz

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]

JST = tz.gettz("Asia/Tokyo")
UTC = tz.gettz("UTC")

def now():
    return dt.datetime.now(tz=JST)

def window(run):
    end = run.replace(hour=7, minute=0, second=0, microsecond=0)
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
        timeout=30
    )
    return len(r.json().get("results", [])) > 0

def create(title, muni, link, published):
    key = dup_key(muni, link)
    if exists(key):
        return

    props = {
        "タイトル": {"title": [{"text": {"content": title[:200]}}]},
        "自治体": {"select": {"name": muni}},
        "URL": {"url": link},
        "取得日時": {"date": {"start": now().isoformat()}},
        "重複キー": {"rich_text": [{"text": {"content": key}}]},
    }
    if published:
        props["発行日"] = {"date": {"start": published.isoformat()}}

    requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers(),
        json={"parent": {"database_id": DB_ID}, "properties": props},
        timeout=30
    )

def main():
    start, end = window(now())

    # TODO: 北海道庁RSSの正確なURLに後で差し替え
    feed_url = "https://example.com/rss"

    feed = feedparser.parse(feed_url)
    for e in feed.entries:
        if not hasattr(e, "published_parsed"):
            continue
        pub = dt.datetime(*e.published_parsed[:6], tzinfo=UTC).astimezone(JST)
        if start <= pub < end:
            create(e.title, "北海道庁", e.link, pub)

if __name__ == "__main__":
    main()
