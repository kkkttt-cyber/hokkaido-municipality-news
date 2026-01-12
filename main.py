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

SOURCES_CSV = "sources.csv"
USER_AGENT = "Mozilla/5.0 (compatible; HokkaidoNewsBot/1.1; +https://github.com/)"

# Notion property names (DB側と一致させてください)
PROP_TITLE = "タイトル"     # Title
PROP_MUNI = "自治体"       # Rich text
PROP_URL = "URL"           # URL
PROP_FETCHED = "取得日時"  # Date
PROP_PUB = "発行日"        # Date
PROP_KEY = "重複キー"      # Rich text

# Date regex (日本語/スラッシュ/ハイフン)
DATE_RE_JP = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
DATE_RE_SLASH = re.compile(r"(\d{4})[\/\.](\d{1,2})[\/\.](\d{1,2})")
DATE_RE_DASH = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")

ATOM_NS = "http://www.w3.org/2005/Atom"
DC_NS = "http://purl.org/dc/elements/1.1/"

# RSSっぽさ判定（URLではなく中身で判定する）
RSS_MARKERS = (b"<rss", b"<feed", b"<rdf", b"xmlns=\"http://www.w3.org/2005/Atom\"")

# =========================
# Time window
# =========================
def now_jst() -> dt.datetime:
    return dt.datetime.now(tz=JST)

def window_24h(run_time: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    """
    対象期間：前日 0:00 〜 当日 0:00（JST）
    """
    end = run_time.replace(hour=0, minute=0, second=0, microsecond=0)
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

def to_jst(d: dt.datetime) -> dt.datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=JST)
    return d.astimezone(JST)

def safe_date_iso(d: dt.datetime | None) -> str:
    return d.astimezone(JST).isoformat() if d else ""

def dup_key(muni: str, url: str, published: dt.datetime | None, title: str) -> str:
    """
    ★積み重ねのための重複キー
    - muni + url + 発行日(YYYY-MM-DD) + タイトル（先頭200）
    → 同じURLでも「日付やタイトルが違えば別レコード」になり、日次で積み上がる
    """
    pub = published.date().isoformat() if published else ""
    base = f"{muni}|{url}|{pub}|{title[:200]}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def notion_exists(key: str) -> bool:
    r = requests.post(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        headers=notion_headers(),
        json={"filter": {"property": PROP_KEY, "rich_text": {"equals": key}}},
        timeout=30,
    )
    r.raise_for_status()
    return len(r.json().get("results", [])) > 0

def notion_create(title: str, muni: str, link: str, published: dt.datetime | None, fetched: dt.datetime) -> bool:
    key = dup_key(muni, link, published, title)
    if notion_exists(key):
        return False

    props = {
        PROP_TITLE: {"title": [{"text": {"content": title[:200]}}]},
        PROP_MUNI: {"rich_text": [{"text": {"content": muni[:200]}}]},
        PROP_URL: {"url": link},
        PROP_FETCHED: {"date": {"start": fetched.isoformat()}},
        PROP_KEY: {"rich_text": [{"text": {"content": key}}]},
    }
    if published is not None:
        props[PROP_PUB] = {"date": {"start": published.isoformat()}}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(),
        json={"parent": {"database_id": DB_ID}, "properties": props},
        timeout=30,
    )

    # ★400などの原因をログに出す（函館の特定に使う）
    if r.status_code >= 400:
        print("[ERROR] notion_create_failed", r.status_code, r.text)

    r.raise_for_status()
    return True

# =========================
# Fetch (bytes decode)
# =========================
XML_DECL_RE = re.compile(br'^\s*<\?xml[^>]*encoding=["\']([^"\']+)["\']', re.I)

def fetch_bytes(url: str) -> tuple[bytes | None, str | None]:
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        if r.status_code != 200:
            return None, None
        ctype = r.headers.get("Content-Type")
        return r.content, ctype
    except Exception:
        return None, None

def decode_bytes(content: bytes, ctype: str | None) -> str:
    """
    文字化け対策：
    - XML宣言 encoding / HTTPヘッダ / 推定 の順でdecode
    """
    # 1) XML宣言のencoding
    m = XML_DECL_RE.search(content[:200])
    if m:
        enc = m.group(1).decode("ascii", errors="ignore") or None
        if enc:
            try:
                return content.decode(enc, errors="replace")
            except Exception:
                pass

    # 2) Content-Typeにcharsetがあれば拾う
    if ctype and "charset=" in ctype.lower():
        try:
            enc = ctype.lower().split("charset=")[-1].split(";")[0].strip()
            if enc:
                return content.decode(enc, errors="replace")
        except Exception:
            pass

    # 3) 最後にUTF-8
    return content.decode("utf-8", errors="replace")

def looks_like_rss(content: bytes, ctype: str | None) -> bool:
    if ctype:
        lc = ctype.lower()
        if "xml" in lc or "rss" in lc or "atom" in lc:
            return True
    head = content.lstrip()[:800].lower()
    return any(m in head for m in RSS_MARKERS)

# =========================
# Date parsing
# =========================
def parse_any_date(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    t = str(text).strip()
    if not t:
        return None

    m = DATE_RE_JP.search(t)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.datetime(y, mo, d, 0, 0, 0, tzinfo=JST)

    m = DATE_RE_SLASH.search(t)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.datetime(y, mo, d, 0, 0, 0, tzinfo=JST)

    m = DATE_RE_DASH.search(t)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.datetime(y, mo, d, 0, 0, 0, tzinfo=JST)

    return None

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
def collect_rss(muni: str, feed_url: str, content: bytes, ctype: str | None,
                start: dt.datetime, end: dt.datetime, fetched: dt.datetime) -> int:
    xml_text = decode_bytes(content, ctype)

    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        print(f"[WARN] {muni} rss_parse_failed url={feed_url} err={e}")
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

        link = get_rss_link(it, feed_url)
        if not link:
            continue

        published = get_rss_published(it)
        if published is None:
            # RSSで発行日が取れない場合は、積み上げ運用の一貫性のためスキップ（必要ならここをfetchedに寄せてもOK）
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
def extract_date_near_anchor(a) -> dt.datetime | None:
    """
    HTML用：リンク周辺（親要素・近傍テキスト）から日付を拾う
    """
    # 1) a自体のテキスト
    d = parse_any_date(a.get_text(" ", strip=True))
    if d:
        return d

    # 2) 親要素を数段さかのぼって、ブロック内テキストから拾う
    node = a
    for _ in range(4):
        node = node.parent
        if not node:
            break
        txt = node.get_text(" ", strip=True)
        d = parse_any_date(txt)
        if d:
            return d

    # 3) 直前のテキスト（現行のfind_previousより軽量）
    prev = a.find_previous(string=True)
    if prev:
        d = parse_any_date(prev)
        if d:
            return d

    # 4) 最後の手段：DATE_RE_JPが見える直前を探す
    prev_jp = a.find_previous(string=DATE_RE_JP)
    d = parse_any_date(prev_jp)
    return d

def collect_html(muni: str, page_url: str, content: bytes, ctype: str | None,
                 start: dt.datetime, end: dt.datetime, fetched: dt.datetime) -> int:
    html = decode_bytes(content, ctype)
    soup = BeautifulSoup(html, "html.parser")

    created = 0

    for a in soup.select("a[href]"):
        title = a.get_text(" ", strip=True)
        href = a.get("href")
        if not title or not href:
            continue

        link = requests.compat.urljoin(page_url, href)

        published = extract_date_near_anchor(a)
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

        content, ctype = fetch_bytes(url)
        if content is None:
            print(f"[WARN] {muni} fetch_failed url={url}")
            print(f"[INFO] {muni} created=0")
            continue

        # ★URLではなく「中身」でRSS/HTMLを自動判別する
        try:
            if looks_like_rss(content, ctype):
                c = collect_rss(muni, url, content, ctype, start, end, fetched)
            else:
                c = collect_html(muni, url, content, ctype, start, end, fetched)
        except Exception as e:
            print(f"[WARN] {muni} collector_failed url={url} err={e}")
            c = 0

        total_created += c
        print(f"[INFO] {muni} created={c}")

    print(f"[DONE] total_created={total_created}")

if __name__ == "__main__":
    main()
