#!/usr/bin/env python3
"""
Fetch recent astro-ph.CO submissions from the arXiv API and merge them into a
durable JSON feed.

Dependency-free: standard library only, so the GitHub Action needs no pip step.

Design notes, after the first run was rate-limited:

* One request, not four. A 20-day window of astro-ph.CO is roughly 350-450
  records, and the arXiv API accepts max_results up to 2000, so the whole
  window normally arrives in a single call. Paging is kept only as a fallback
  when arXiv reports more results than one page returned, and it waits the
  required 3 seconds between calls.
* HTTPS directly. The previous version used http:// and ate a 302 redirect on
  every call, doubling the request count for no reason.
* Rate limiting is respected, not worked around. On 429 the script honours the
  Retry-After header when present, otherwise backs off 60s, 180s, 420s. arXiv's
  terms forbid spreading requests across machines to evade limits, so there is
  deliberately no proxy or retry-elsewhere path here.
* The feed is merged, not overwritten. Records are keyed by arXiv id and kept
  for RETAIN_DAYS. A failed run therefore loses nothing, and a run after an
  outage backfills. The previous version rewrote the file each time, so any
  paper older than the window was gone forever.
* On total failure the existing feed file is left untouched and the script
  exits non-zero. A red build means "no new data", never "corrupted data".

arXiv API docs:  https://info.arxiv.org/help/api/user-manual.html
arXiv API terms: https://info.arxiv.org/help/api/tou.html
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

API = "https://export.arxiv.org/api/query"
CATEGORY = os.environ.get("ARXIV_CATEGORY", "astro-ph.CO")
WINDOW_DAYS = int(os.environ.get("ARXIV_WINDOW_DAYS", "20"))
RETAIN_DAYS = int(os.environ.get("ARXIV_RETAIN_DAYS", "45"))
PAGE_SIZE = int(os.environ.get("ARXIV_PAGE_SIZE", "600"))
MAX_RECORDS = int(os.environ.get("ARXIV_MAX_RECORDS", "2000"))
OUT_PATH = os.environ.get("ARXIV_OUT", f"feed/{CATEGORY}.json")
REQUEST_GAP = float(os.environ.get("ARXIV_REQUEST_GAP", "3"))
BACKOFF = [60, 180, 420]
USER_AGENT = os.environ.get(
    "ARXIV_USER_AGENT",
    "arxiv-daily-digest/2.0 (+https://github.com/adam-gomulka/arxiv-fetch)",
)

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def build_query(start_dt, end_dt):
    """arXiv date-range syntax is submittedDate:[YYYYMMDDHHMM TO YYYYMMDDHHMM]."""
    lo = start_dt.strftime("%Y%m%d%H%M")
    hi = end_dt.strftime("%Y%m%d%H%M")
    return f"cat:{CATEGORY} AND submittedDate:[{lo} TO {hi}]"


def retry_after_seconds(exc, default):
    """Honour Retry-After when arXiv sends one; it may be seconds or a date."""
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
        from email.utils import parsedate_to_datetime
        when = parsedate_to_datetime(raw)
        delta = (when - datetime.now(timezone.utc)).total_seconds()
        return max(1, min(int(delta), 900))
    except Exception:  # noqa: BLE001
        return default


def fetch_page(query, start, page_size, _sleep=time.sleep):
    params = {
        "search_query": query,
        "start": str(start),
        "max_results": str(page_size),
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    waits = list(BACKOFF)          # local copy: never mutate module state
    attempts = len(waits) + 1

    for i in range(attempts):
        if i > 0:
            log(f"  backing off {waits[i - 1]}s before attempt {i + 1}")
            _sleep(waits[i - 1])
        last = i == attempts - 1
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and not last:
                # Retry-After, when arXiv sends one, overrides our schedule.
                waits[i] = retry_after_seconds(exc, waits[i])
                log(f"  429 Too Many Requests (attempt {i + 1}); next wait {waits[i]}s")
                continue
            if 500 <= exc.code < 600 and not last:
                log(f"  HTTP {exc.code} (attempt {i + 1})")
                continue
            raise
        except Exception as exc:  # noqa: BLE001
            if last:
                raise
            log(f"  request failed: {exc} (attempt {i + 1})")
            continue
    raise RuntimeError("unreachable")


def text_of(entry, path):
    node = entry.find(path, NS)
    if node is None or node.text is None:
        return ""
    return " ".join(node.text.split())


def parse_entries(xml_bytes):
    root = ET.fromstring(xml_bytes)
    total_node = root.find("opensearch:totalResults", NS)
    total = int(total_node.text) if total_node is not None and total_node.text else None

    records = []
    for entry in root.findall("atom:entry", NS):
        raw_id = text_of(entry, "atom:id")
        arxiv_id = raw_id.rsplit("/abs/", 1)[-1] if "/abs/" in raw_id else raw_id
        version = ""
        if "v" in arxiv_id.rsplit(".", 1)[-1]:
            arxiv_id, _, version = arxiv_id.partition("v")

        primary = entry.find("arxiv:primary_category", NS)
        categories = [c.get("term") for c in entry.findall("atom:category", NS) if c.get("term")]

        pdf = ""
        for link in entry.findall("atom:link", NS):
            if link.get("title") == "pdf":
                pdf = link.get("href", "")

        records.append({
            "id": arxiv_id,
            "version": version,
            "title": text_of(entry, "atom:title"),
            "authors": [
                " ".join((a.findtext("atom:name", default="", namespaces=NS) or "").split())
                for a in entry.findall("atom:author", NS)
            ],
            "abstract": text_of(entry, "atom:summary"),
            "primary_category": primary.get("term") if primary is not None else "",
            "categories": categories,
            "cross_listed": bool(primary is not None and primary.get("term") != CATEGORY),
            "published": text_of(entry, "atom:published"),
            "updated": text_of(entry, "atom:updated"),
            "comment": text_of(entry, "arxiv:comment"),
            "journal_ref": text_of(entry, "arxiv:journal_ref"),
            "doi": text_of(entry, "arxiv:doi"),
            "abs_url": f"https://arxiv.org/abs/{arxiv_id}",
            "pdf_url": pdf,
        })
    return records, total


def load_existing(path):
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return {r["id"]: r for r in data.get("records", []) if r.get("id")}


def merge(existing, fetched):
    """Newly fetched records win; anything previously stored is preserved."""
    merged = dict(existing)
    added = 0
    for rec in fetched:
        if rec["id"] not in merged:
            added += 1
        merged[rec["id"]] = rec
    return merged, added


def prune(records, now, retain_days):
    cutoff = now - timedelta(days=retain_days)
    kept, dropped = [], 0
    for rec in records:
        stamp = rec.get("published", "")
        try:
            when = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            kept.append(rec)  # unparseable date: keep rather than silently discard
            continue
        if when >= cutoff:
            kept.append(rec)
        else:
            dropped += 1
    return kept, dropped


def collect(query, now):
    fetched, seen = [], set()
    start = 0
    expected_total = None

    while start < MAX_RECORDS:
        log(f"  requesting {start}..{start + PAGE_SIZE}")
        page, total = parse_entries(fetch_page(query, start, PAGE_SIZE))
        if expected_total is None:
            expected_total = total
            log(f"  arXiv reports {total} total results for the window")
        if not page:
            break
        new_this_page = 0
        for rec in page:
            if rec["id"] not in seen:
                seen.add(rec["id"])
                fetched.append(rec)
                new_this_page += 1
        if new_this_page == 0:
            # Overlapping or repeated pages: without this the loop can spin
            # MAX_RECORDS/PAGE_SIZE times, sleeping 3s each, making no progress.
            log("  page added no new records; stopping to avoid a no-progress loop")
            break
        if expected_total is not None and len(fetched) >= min(expected_total, MAX_RECORDS):
            break
        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        log(f"  window needs another page; waiting {REQUEST_GAP}s")
        time.sleep(REQUEST_GAP)

    return fetched, expected_total


def main():
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=WINDOW_DAYS)
    query = build_query(start_dt, now)
    log(f"query: {query}")

    existing = load_existing(OUT_PATH)
    log(f"existing feed holds {len(existing)} records")

    try:
        fetched, expected_total = collect(query, now)
    except Exception as exc:  # noqa: BLE001
        log(f"FATAL: arXiv fetch failed: {exc}")
        log("existing feed left untouched; exiting non-zero")
        return 1

    merged_map, added = merge(existing, fetched)
    records = sorted(merged_map.values(), key=lambda r: r.get("published", ""), reverse=True)
    records, dropped = prune(records, now, RETAIN_DAYS)

    window_complete = (
        expected_total is None or len(fetched) >= min(expected_total, MAX_RECORDS)
    )

    payload = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "arxiv-api",
        "category": CATEGORY,
        "window_start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": WINDOW_DAYS,
        "retain_days": RETAIN_DAYS,
        "fetched_this_run": len(fetched),
        "added_this_run": added,
        "pruned_this_run": dropped,
        "reported_total_for_window": expected_total,
        "window_complete": window_complete,
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
        f"({len(fetched)} fetched, {added} new, {dropped} pruned), "
        f"window_complete={window_complete}"
    )
    if not window_complete:
        log("WARNING: fetched fewer records than arXiv reported for the window")
    return 0


if __name__ == "__main__":
    sys.exit(main())
