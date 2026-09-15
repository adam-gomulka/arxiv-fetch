#!/usr/bin/env python3
"""
Fetch recent astro-ph.CO submissions from the arXiv API and write them as JSON.

Dependency-free: standard library only, so the GitHub Action needs no pip step.

The script fetches a rolling window (default 10 days) on every run and rewrites
the feed file completely. That makes each run idempotent and self-contained:
there is no incremental state to corrupt, and a consumer that missed a week can
still read everything it needs from the same file.

arXiv API docs: https://info.arxiv.org/help/api/user-manual.html
Rate limit: arXiv asks for no more than one request every 3 seconds.
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

API = "http://export.arxiv.org/api/query"
CATEGORY = os.environ.get("ARXIV_CATEGORY", "astro-ph.CO")
WINDOW_DAYS = int(os.environ.get("ARXIV_WINDOW_DAYS", "10"))
PAGE_SIZE = int(os.environ.get("ARXIV_PAGE_SIZE", "100"))
MAX_RECORDS = int(os.environ.get("ARXIV_MAX_RECORDS", "2000"))
OUT_PATH = os.environ.get("ARXIV_OUT", f"feed/{CATEGORY}.json")
USER_AGENT = os.environ.get(
    "ARXIV_USER_AGENT",
    "arxiv-daily-digest/1.0 (GitHub Actions; contact via repository issues)",
)

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


def build_query(start_dt, end_dt):
    """arXiv date-range syntax is submittedDate:[YYYYMMDDHHMM TO YYYYMMDDHHMM]."""
    lo = start_dt.strftime("%Y%m%d%H%M")
    hi = end_dt.strftime("%Y%m%d%H%M")
    return f"cat:{CATEGORY} AND submittedDate:[{lo} TO {hi}]"


def fetch_page(query, start, page_size, attempt=1):
    params = {
        "search_query": query,
        "start": str(start),
        "max_results": str(page_size),
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except Exception as exc:  # noqa: BLE001
        if attempt >= 4:
            raise
        backoff = 5 * attempt
        print(f"  request failed ({exc}); retrying in {backoff}s", file=sys.stderr)
        time.sleep(backoff)
        return fetch_page(query, start, page_size, attempt + 1)


def text_of(entry, path):
    node = entry.find(path, NS)
    if node is None or node.text is None:
        return ""
    return " ".join(node.text.split())


def parse_entries(xml_bytes):
    root = ET.fromstring(xml_bytes)
    total_node = root.find("opensearch:totalResults", {
        "opensearch": "http://a9.com/-/spec/opensearch/1.1/"
    })
    total = int(total_node.text) if total_node is not None and total_node.text else None

    records = []
    for entry in root.findall("atom:entry", NS):
        raw_id = text_of(entry, "atom:id")
        arxiv_id = raw_id.rsplit("/abs/", 1)[-1] if "/abs/" in raw_id else raw_id
        version = ""
        if "v" in arxiv_id.rsplit(".", 1)[-1]:
            base, _, version = arxiv_id.partition("v")
            arxiv_id = base

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


def main():
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=WINDOW_DAYS)
    query = build_query(start_dt, now)
    print(f"query: {query}", file=sys.stderr)

    records = []
    seen = set()
    start = 0
    expected_total = None

    while start < MAX_RECORDS:
        print(f"  fetching {start}..{start + PAGE_SIZE}", file=sys.stderr)
        page, total = parse_entries(fetch_page(query, start, PAGE_SIZE))
        if expected_total is None:
            expected_total = total
            print(f"  arXiv reports {total} total results", file=sys.stderr)
        if not page:
            break
        for rec in page:
            if rec["id"] not in seen:
                seen.add(rec["id"])
                records.append(rec)
        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(3)  # arXiv asks for one request per 3 seconds

    records.sort(key=lambda r: r["published"], reverse=True)

    complete = expected_total is None or len(records) >= min(expected_total, MAX_RECORDS)
    payload = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "arxiv-api",
        "category": CATEGORY,
        "window_start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": WINDOW_DAYS,
        "reported_total": expected_total,
        "record_count": len(records),
        "complete": complete,
        "records": records,
    }

    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, ensure_ascii=False)
        fh.write("\n")

    print(
        f"wrote {OUT_PATH}: {len(records)} records, "
        f"reported_total={expected_total}, complete={complete}",
        file=sys.stderr,
    )
    if not complete:
        print("WARNING: fetched fewer records than arXiv reported", file=sys.stderr)


if __name__ == "__main__":
    main()
