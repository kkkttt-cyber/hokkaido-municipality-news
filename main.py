import os
import csv
import re
import hashlib
import datetime as dt
import requests
from bs4 import BeautifulSoup
from dateutil import tz
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

# =========================
# Config
# =========================
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]

JST = tz.gettz("Asia/Tokyo")
DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

SOURCES_CSV = "sources.csv"
USER_AGENT = "Mozilla/5.0 (compatible; HokkaidoNewsBot/1.0; +https://github.com/)"

ATOM_NS = "http://www.w3.org/2005/Atom"
DC_NS = "http://purl.org/dc/elements/1.1/"

# =========================
# Time window
# =========================
def now_jst() -> dt.datetime:
    return dt.datetime.now(tz=JST)

def window_24h(run_time: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    """
    対象期間：前日 9:00 〜 当日 9:00（JST）
    """
    end = run_time.replace(hour=9, minute=0, second=0, microsecond=0)
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
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    y, mo, d = map(int, m.groups())
    return dt.datetime(y, mo, d, 0, 0, 0, tzinfo=JST)

def to_jst(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=JST)
    return d.astimezone(JST)

def parse_rss_date(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    t = text.strip()

    try:
        d = parsedate_to_datetime(t)
        return to_jst(d)
    except Exception:
        pass

    try:
        t2 = t.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(t2)
        return to_jst(d)
    except Exception:
        return None

# =========================
# Fetch (HTML/RSS common)  ★文字化け対策入り
# =========================
XML_DECL_RE = re.compile(br'^\s*<\?xml[^>]*encoding=["\']([^"\']+)["\']', re.I)

def fetch_text(url: str) -> str | None:
    """
    RSS/XMLの文字化け対策：
    - requestsの r.text に頼らず bytes で受ける
    - XML宣言 encoding / HTTPヘッダ / apparent_encoding の順でdecode
    """
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        if r.status_code != 200:
            return None

        content = r.content

        # 1) XML宣言のencoding（例: Shift_JIS / UTF-8）
        m = XML_DECL_RE.search(content[:200])
        if m:
            enc = m.group(1).decode("ascii", errors="ignore") or None
            if enc:
                try:
                    return content.decode(enc, errors="replace")
                except Exception:
                    pass

        # 2) HTTPヘッダ由来
        if r.encoding:
            try:
                return content.decode(r.encoding, errors="replace")
            except Exception:
                pass

        # 3) 推定
        try:
            enc = r.apparent_encoding  # type: ignore
            if enc:
                return content.decode(enc, errors="replace")
        except Exception:
            pass

        # 4) 最後にUTF-8
        return content.decode("utf-8", errors="replace")

    except Exception:
        return None

def is_rss_url(url: str) -> bool:
    u = url.lower()
    return (
        u.endswith(".rss")
        or u.endswith(".rdf")
        or u.endswith(".xml")
        or "index.rss" in u
        or "news.rss" in u
    )

# =========================
# RSS helpers
# =========================
def first_text(elem, candidates: list[str]) -> str | None:
    for tag in candidates:
        found = elem.find(tag)
        if found is not None and found.text:
            return found.text.strip()
    return None

def find_first_by_localname(elem, localname: str) -> str | None:
    for child in elem.iter():
        if isinstance(child.tag, str) and child.tag.endswith(localname):
            if child.text and child.text.strip():
                return child.text.strip()
    return None

def get_rss_link(it, feed_url: str) -> str | None:
    link = first_text(it, ["link"])

    # Atom: <link href="...">
    if not link:
        atom_link = it.find(f"{{{ATOM_NS}}}link")
        if atom_link is not None:
            href = atom_link.attrib.get("href")
            if href:
                link = href.strip()

    # RSS: <guid>がURLの場合
    if not link:
        guid = first_text(it, ["guid"])
        if guid:
            link = guid.strip()

    if not link:
        return None

    return requests.compat.urljoin(feed_url, link)

def get_rss_published(it) -> dt.datetime | None:
    pub = (
        first_text(it, ["pubDate"])
        or first_text(it, [f"{{{DC_NS}}}date"])
        or first_text(it, [f"{{{ATOM_NS}}}updated"])
        or first_text(it, [f"{{{ATOM_NS}}}published"])
    )

    if not pub:
        for ln in ["pubDate", "date", "updated", "published"]:
            pub = find_first_by_localname(it, ln)
            if pub:
                break

    return parse_rss_date(pub)

# =========================
# Collector (RSS)
# =========================
def collect_rss(muni: str, url: str, start: dt.datetime, end: dt.datetime, fetched: dt.datetime) -> int:
    xml_text = fetch_text(url)
    if xml_text is None:
        print(f"[WARN] {muni} fetch_failed url={url}")
        return 0

    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        print(f"[WARN] {muni} rss_parse_failed url={url} err={e}")
        return 0

    created = 0

    items = root.findall(".//item")
    if not items:
        items = root.findall(f".//{{{ATOM_NS}}}entry")

    for it in items:
        title = first_text(it, ["title", f"{{{ATOM_NS}}}title"]) or ""
        title = title.strip()
        if not title:
            continue

        link = get_rss_link(it, url)
        if not link:
            continue

        published = get_rss_published(it)
        if published is None:
            continue

        if not (start <= published < end):
            continue

        try:
            if notion_create(title, muni, link, published, fetched):
                created += 1
        except Exception as e:
            print(f"[WARN] {muni} notion_failed link={link} err={e}")

    return created

# =========================
# Collector (HTML)
# =========================
def collect_html(muni: str, url: str, start: dt.datetime, end: dt.datetime, fetched: dt.datetime) -> int:
    html = fetch_text(url)
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

        prev_date_text = a.find_previous(string=DATE_RE)
        published = parse_jp_date(prev_date_text)

        if published is None:
            continue

        if not (start <= published < end):
            continue

        try:
            if notion_create(title, muni, link, published, fetched):
                created += 1
        except Exception as e:
            print(f"[WARN] {muni} notion_failed link={link} err={e}")

    return created

# =========================
# Main
# =========================
def read_sources(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
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

        if is_rss_url(url):
            c = collect_rss(muni, url, start, end, fetched)
        else:
            c = collect_html(muni, url, start, end, fetched)

        total_created += c
        print(f"[INFO] {muni} created={c}")

    print(f"[DONE] total_created={total_created}")

if __name__ == "__main__":
    main()
