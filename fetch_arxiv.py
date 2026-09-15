#!/usr/bin/env python3
"""
Build a durable JSON feed of recent astro-ph.CO papers.

Standard library only, so the GitHub Action needs no pip step.

WHY THERE ARE THREE SOURCES
---------------------------
The arXiv API refuses requests from GitHub-hosted runners: two runs twenty
minutes apart returned 429 on the very first request, then 503, despite a
single request per run and full Retry-After compliance. GitHub runners sit on
shared cloud address space, so the throttle is earned by other traffic on the
same IP. arXiv's terms forbid spreading requests across machines to evade a
limit, so there is deliberately no proxy or IP-rotation path here. The answer
is to ask a different, lighter door: the RSS feed, which is one small request
per day and is what syndication is for.

Sources are tried in order and their results are MERGED, so partial coverage
from one is topped up by another:

  arxiv-rss  one request. Complete for the day just announced, nothing older.
  inspire    windowed and good for backfill, but indexes only the HEP-relevant
             slice of astro-ph.CO, so roughly 60-75% of the category.
  arxiv-api  complete and windowed, but blocked from GitHub runners. OFF by
             default; set ARXIV_TRY_API=1 to attempt it (useful if you ever run
             this from a machine arXiv will talk to).

A source that raises, or returns nothing, is logged and skipped. The run fails
only if every source yields nothing, and in that case the existing feed file is
left untouched. A red build means "no new data", never "corrupted data".

Records are merged by arXiv id into a rolling archive retained for RETAIN_DAYS,
so a failed day loses nothing and coverage improves over successive runs.

arXiv RSS help:  https://info.arxiv.org/help/rss.html
arXiv API terms: https://info.arxiv.org/help/api/tou.html
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

CATEGORY = os.environ.get("ARXIV_CATEGORY", "astro-ph.CO")
WINDOW_DAYS = int(os.environ.get("ARXIV_WINDOW_DAYS", "20"))
RETAIN_DAYS = int(os.environ.get("ARXIV_RETAIN_DAYS", "45"))
OUT_PATH = os.environ.get("ARXIV_OUT", f"feed/{CATEGORY}.json")
TRY_API = os.environ.get("ARXIV_TRY_API", "0") == "1"
REQUEST_GAP = float(os.environ.get("ARXIV_REQUEST_GAP", "3"))
BACKOFF = [60, 180, 420]
USER_AGENT = os.environ.get(
    "ARXIV_USER_AGENT",
    "arxiv-daily-digest/3.0 (+https://github.com/adam-gomulka/arxiv-fetch)",
)

RSS_URL = f"https://rss.arxiv.org/rss/{CATEGORY}"
INSPIRE_URL = "https://inspirehep.net/api/literature"
API_URL = "https://export.arxiv.org/api/query"

ARXIV_ID_RE = re.compile(r"(\d{4}\.\d{4,5})(?:v(\d+))?")


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def localname(tag):
    """'{http://purl.org/dc/elements/1.1/}creator' -> 'creator'."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def child_text(node, name):
    for kid in node:
        if localname(kid.tag) == name:
            return " ".join((kid.text or "").split())
    return ""


def children_text(node, name):
    out = []
    for kid in node:
        if localname(kid.tag) == name:
            out.append(" ".join((kid.text or "").split()))
    return out


def retry_after_seconds(exc, default):
    try:
        raw = exc.headers.get("Retry-After")
    except Exception:  # noqa: BLE001
        return default
    if not raw:
        return default
    raw = raw.strip()
    if raw.isdigit():
        return min(int(raw), 900)
    try:
        when = parsedate_to_datetime(raw)
        return max(1, min(int((when - datetime.now(timezone.utc)).total_seconds()), 900))
    except Exception:  # noqa: BLE001
        return default


def http_get(url, backoff=None, _sleep=time.sleep, timeout=120):
    """GET with polite retry. Honours Retry-After; never routes around a limit."""
    waits = list(BACKOFF if backoff is None else backoff)
    attempts = len(waits) + 1
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for i in range(attempts):
        if i > 0:
            log(f"    backing off {waits[i - 1]}s before attempt {i + 1}")
            _sleep(waits[i - 1])
        last = i == attempts - 1
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and not last:
                waits[i] = retry_after_seconds(exc, waits[i])
                log(f"    429 Too Many Requests (attempt {i + 1}); next wait {waits[i]}s")
                continue
            if 500 <= exc.code < 600 and not last:
                log(f"    HTTP {exc.code} (attempt {i + 1})")
                continue
            raise
        except Exception as exc:  # noqa: BLE001
            if last:
                raise
            log(f"    request failed: {exc} (attempt {i + 1})")
            continue
    raise RuntimeError("unreachable")


def blank_record(arxiv_id, version=""):
    return {
        "id": arxiv_id,
        "version": version,
        "title": "",
        "authors": [],
        "abstract": "",
        "primary_category": "",
        "categories": [],
        "cross_listed": False,
        "announce_type": "",
        "published": "",
        "updated": "",
        "comment": "",
        "journal_ref": "",
        "doi": "",
        "abs_url": f"https://arxiv.org/abs/{arxiv_id}",
        "pdf_url": f"https://arxiv.org/pdf/{arxiv_id}",
        "source": "",
    }


# --------------------------------------------------------------------------
# Source 1: arXiv RSS
# --------------------------------------------------------------------------

def parse_rss(xml_bytes):
    root = ET.fromstring(xml_bytes)
    records = []
    for item in root.iter():
        if localname(item.tag) != "item":
            continue

        desc = child_text(item, "description")
        guid = child_text(item, "guid")
        link = child_text(item, "link")

        match = ARXIV_ID_RE.search(desc) or ARXIV_ID_RE.search(guid) or ARXIV_ID_RE.search(link)
        if not match:
            continue
        arxiv_id, version = match.group(1), match.group(2) or ""

        rec = blank_record(arxiv_id, version)

        title = child_text(item, "title")
        # Defensive: older feeds appended "(arXiv:XXXX.XXXXX [cat])" to titles.
        rec["title"] = re.sub(r"\s*\(arXiv:\S+\s*\[[^\]]+\]\)\s*$", "", title).strip()

        abstract = desc
        if "Abstract:" in desc:
            abstract = desc.split("Abstract:", 1)[1]
        rec["abstract"] = abstract.strip()

        creators = child_text(item, "creator")
        rec["authors"] = [a.strip() for a in creators.split(",") if a.strip()]

        cats = children_text(item, "category")
        rec["categories"] = cats
        rec["primary_category"] = cats[0] if cats else ""

        announce = child_text(item, "announce_type").lower()
        if not announce:
            m = re.search(r"Announce Type:\s*([\w-]+)", desc)
            announce = m.group(1).lower() if m else ""
        rec["announce_type"] = announce
        rec["cross_listed"] = announce.startswith("cross")

        pub = child_text(item, "pubDate")
        if pub:
            try:
                rec["published"] = (
                    parsedate_to_datetime(pub)
                    .astimezone(timezone.utc)
                    .strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            except Exception:  # noqa: BLE001
                pass
        rec["updated"] = rec["published"]
        rec["source"] = "arxiv-rss"
        records.append(rec)
    return records


def source_rss(_now):
    log(f"  GET {RSS_URL}")
    return parse_rss(http_get(RSS_URL))


# --------------------------------------------------------------------------
# Source 2: INSPIRE-HEP
# --------------------------------------------------------------------------

def parse_inspire(payload):
    data = json.loads(payload)
    hits = (data.get("hits") or {}).get("hits") or []
    total = (data.get("hits") or {}).get("total")
    records = []

    for hit in hits:
        meta = hit.get("metadata") or {}
        eprints = meta.get("arxiv_eprints") or []
        if not eprints:
            continue
        raw = (eprints[0].get("value") or "").strip()
        match = ARXIV_ID_RE.search(raw)
        if not match:
            continue
        rec = blank_record(match.group(1), match.group(2) or "")

        titles = meta.get("titles") or []
        rec["title"] = " ".join((titles[0].get("title") or "").split()) if titles else ""

        abstracts = meta.get("abstracts") or []
        rec["abstract"] = (
            " ".join((abstracts[0].get("value") or "").split()) if abstracts else ""
        )

        rec["authors"] = [
            a.get("full_name", "") for a in (meta.get("authors") or []) if a.get("full_name")
        ]

        cats = eprints[0].get("categories") or []
        rec["categories"] = cats
        rec["primary_category"] = cats[0] if cats else ""
        rec["cross_listed"] = bool(cats) and cats[0] != CATEGORY

        date = meta.get("earliest_date") or ""
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            rec["published"] = f"{date}T00:00:00Z"
        rec["updated"] = rec["published"]

        dois = meta.get("dois") or []
        rec["doi"] = dois[0].get("value", "") if dois else ""
        rec["source"] = "inspire"
        records.append(rec)

    return records, total


def source_inspire(now):
    start = (now - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")
    query = f"arxiv_eprints.categories:{CATEGORY} and de {start}->{end}"
    records, page = [], 1

    while page <= 10:
        params = {
            "sort": "mostrecent",
            "size": "200",
            "page": str(page),
            "q": query,
            "fields": "titles,abstracts,arxiv_eprints,authors,earliest_date,dois",
        }
        url = f"{INSPIRE_URL}?{urllib.parse.urlencode(params)}"
        log(f"  GET inspire page {page}")
        batch, total = parse_inspire(http_get(url))
        if not batch:
            break
        records.extend(batch)
        if total is not None and len(records) >= total:
            break
        if len(batch) < 200:
            break
        page += 1
        time.sleep(REQUEST_GAP)

    return records


# --------------------------------------------------------------------------
# Source 3: arXiv API (opt-in; blocked from GitHub runners)
# --------------------------------------------------------------------------

def parse_api(xml_bytes):
    root = ET.fromstring(xml_bytes)
    records = []
    for entry in root.iter():
        if localname(entry.tag) != "entry":
            continue
        raw_id = child_text(entry, "id")
        match = ARXIV_ID_RE.search(raw_id)
        if not match:
            continue
        rec = blank_record(match.group(1), match.group(2) or "")
        rec["title"] = child_text(entry, "title")
        rec["abstract"] = child_text(entry, "summary")
        rec["published"] = child_text(entry, "published")
        rec["updated"] = child_text(entry, "updated")
        rec["comment"] = child_text(entry, "comment")
        rec["journal_ref"] = child_text(entry, "journal_ref")
        rec["doi"] = child_text(entry, "doi")

        authors = []
        for kid in entry:
            if localname(kid.tag) == "author":
                authors.append(child_text(kid, "name"))
        rec["authors"] = [a for a in authors if a]

        cats, primary = [], ""
        for kid in entry:
            name = localname(kid.tag)
            if name == "category" and kid.get("term"):
                cats.append(kid.get("term"))
            elif name == "primary_category" and kid.get("term"):
                primary = kid.get("term")
        rec["categories"] = cats
        rec["primary_category"] = primary or (cats[0] if cats else "")
        rec["cross_listed"] = bool(primary) and primary != CATEGORY
        rec["source"] = "arxiv-api"
        records.append(rec)
    return records


def source_api(now):
    lo = (now - timedelta(days=WINDOW_DAYS)).strftime("%Y%m%d%H%M")
    hi = now.strftime("%Y%m%d%H%M")
    params = {
        "search_query": f"cat:{CATEGORY} AND submittedDate:[{lo} TO {hi}]",
        "start": "0",
        "max_results": "600",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    log("  GET arxiv api")
    return parse_api(http_get(url))


SOURCES = [("arxiv-rss", source_rss), ("inspire", source_inspire)]
if TRY_API:
    SOURCES.append(("arxiv-api", source_api))


# --------------------------------------------------------------------------
# Archive handling
# --------------------------------------------------------------------------

def load_existing(path):
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return {r["id"]: r for r in data.get("records", []) if r.get("id")}


def better(new, old):
    """Prefer the record carrying more usable content."""
    def score(r):
        return (
            bool(r.get("abstract")),
            bool(r.get("authors")),
            len(r.get("abstract") or ""),
            len(r.get("categories") or []),
        )
    return new if score(new) > score(old) else old


def merge(existing, fetched):
    merged = dict(existing)
    added = 0
    for rec in fetched:
        rid = rec["id"]
        if rid in merged:
            merged[rid] = better(rec, merged[rid])
        else:
            merged[rid] = rec
            added += 1
    return merged, added


def prune(records, now, retain_days):
    cutoff = now - timedelta(days=retain_days)
    kept, dropped = [], 0
    for rec in records:
        stamp = rec.get("published", "")
        try:
            when = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            kept.append(rec)  # unparseable: keep rather than silently discard
            continue
        if when >= cutoff:
            kept.append(rec)
        else:
            dropped += 1
    return kept, dropped


def main():
    now = datetime.now(timezone.utc)
    existing = load_existing(OUT_PATH)
    log(f"existing feed holds {len(existing)} records")

    fetched, report = [], {}
    for name, fn in SOURCES:
        log(f"source {name}:")
        try:
            got = fn(now)
        except Exception as exc:  # noqa: BLE001
            log(f"  FAILED: {exc}")
            report[name] = {"ok": False, "records": 0, "error": str(exc)[:200]}
            continue
        log(f"  got {len(got)} records")
        report[name] = {"ok": bool(got), "records": len(got)}
        if not got:
            report[name]["error"] = "returned no records"
        fetched.extend(got)

    if not fetched:
        log("FATAL: every source returned nothing; existing feed left untouched")
        return 1

    merged_map, added = merge(existing, fetched)
    records = sorted(merged_map.values(), key=lambda r: r.get("published", ""), reverse=True)
    records, dropped = prune(records, now, RETAIN_DAYS)

    payload = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "category": CATEGORY,
        "sources": report,
        "sources_ok": [n for n, r in report.items() if r["ok"]],
        "window_days": WINDOW_DAYS,
        "retain_days": RETAIN_DAYS,
        "fetched_this_run": len(fetched),
        "added_this_run": added,
        "pruned_this_run": dropped,
        "record_count": len(records),
        "oldest_retained": records[-1]["published"] if records else None,
        "newest_retained": records[0]["published"] if records else None,
        "records": records,
    }

    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, ensure_ascii=False)
        fh.write("\n")

    log(
        f"wrote {OUT_PATH}: {len(records)} retained "
        f"({len(fetched)} fetched, {added} new, {dropped} pruned) "
        f"via {payload['sources_ok']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
