
import argparse
import csv
import datetime as dt
from collections import defaultdict, Counter
from typing import Dict, Iterable, List, Optional, Set, Tuple
import time
import random

import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

SEARCH_URL = "https://api.github.com/search/issues"
COMMITS_URL_TMPL = "https://api.github.com/repos/{owner}/{repo}/commits"

def to_iso(d: dt.datetime) -> str:
    return d.isoformat() + "Z"

def month_slices(start: dt.date, end: dt.date, months_per_slice: int = 1) -> List[Tuple[dt.datetime, dt.datetime]]:
    out = []
    cur = dt.date(start.year, start.month, 1)
    end_first = dt.date(end.year, end.month, 1)
    while cur < end_first:
        m = cur.month - 1 + months_per_slice
        y = cur.year + m // 12
        m = m % 12 + 1
        nxt = dt.date(y, m, 1)
        if nxt > end_first:
            nxt = end_first
        out.append((dt.datetime.combine(cur, dt.time.min), dt.datetime.combine(nxt, dt.time.min)))
        cur = nxt
    return out

def rest_paginated(sess: requests.Session, url: str, params: dict, desc: str) -> Iterable[dict]:
    page = 1
    fetched = 0
    while True:
        params["per_page"] = 100
        params["page"] = page
        r = sess.get(url, params=params)
        if r.status_code == 403 and "rate limit" in r.text.lower():
            reset = int(r.headers.get("X-RateLimit-Reset", "0"))
            now = int(time.time())
            wait = max(5, reset - now + 1)
            (tqdm.write if tqdm else print)(f"[rate limit] sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            back = min(30, 2**(page%6)) + random.uniform(0,1.2)
            (tqdm.write if tqdm else print)(f"[{desc}] server {r.status_code}, retry page {page} in {back:.1f}s")
            time.sleep(back)
            continue
        r.raise_for_status()
        arr = r.json()
        if not isinstance(arr, list) or not arr:
            break
        for it in arr:
            fetched += 1
            if tqdm is None and fetched % 300 == 0:
                print(f"  {desc} fetched {fetched} items...", flush=True)
            yield it
        if 'next' not in r.links:
            break
        page += 1

def search_count(sess: requests.Session, q: str) -> int:
    """Return total_count for a query (subject to GitHub Search caps)."""
    r = sess.get(SEARCH_URL, params={"q": q, "per_page": 1, "page": 1})
    if r.status_code == 403 and "rate limit" in r.text.lower():
        reset = int(r.headers.get("X-RateLimit-Reset", "0"))
        now = int(time.time())
        wait = max(5, reset - now + 1)
        (tqdm.write if tqdm else print)(f"[rate limit] sleeping {wait}s")
        time.sleep(wait)
        return search_count(sess, q)
    r.raise_for_status()
    j = r.json()
    return int(j.get("total_count", 0))

def search_items(sess: requests.Session, q: str, desc: str) -> Iterable[dict]:
    """Search issues/PRs fully paginated, stopping before hitting the 1000-item hard cap (page<=10)."""
    page = 1
    fetched = 0
    while True:
        params = {"q": q, "per_page": 100, "page": page}
        r = sess.get(SEARCH_URL, params=params)
        if r.status_code == 403 and "rate limit" in r.text.lower():
            reset = int(r.headers.get("X-RateLimit-Reset", "0"))
            now = int(time.time())
            wait = max(5, reset - now + 1)
            (tqdm.write if tqdm else print)(f"[rate limit] sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            back = min(30, 2**(page%6)) + random.uniform(0,1.2)
            (tqdm.write if tqdm else print)(f"[{desc}] server {r.status_code}, retry page {page} in {back:.1f}s")
            time.sleep(back)
            continue
        if r.status_code == 422:
            # likely page beyond 10 (cap); stop to let caller split further
            break
        r.raise_for_status()
        j = r.json()
        items = j.get("items", [])
        if not items:
            break
        for it in items:
            fetched += 1
            if tqdm is None and fetched % 300 == 0:
                print(f"  {desc} fetched {fetched} items...", flush=True)
            yield it
        if len(items) < 100 or page >= 10:  # don't go beyond the 1000 cap
            break
        page += 1

def adaptive_search_range(sess: requests.Session, base_q_prefix: str, start: dt.date, end: dt.date, desc: str) -> Iterable[dict]:
    """Yield items for a date range, splitting the range until each subrange stays under the 1000-item cap."""
    def _recurse(a: dt.date, b: dt.date):
        q = f"{base_q_prefix} created:{a}..{b}"
        total = search_count(sess, q)
        if total <= 1000:
            yield from search_items(sess, q, f"{desc} {a}..{b}")
        else:
            mid = a + (b - a) // 2
            if mid <= a:  # avoid infinite loop on same day
                yield from search_items(sess, q, f"{desc} {a}..{b}")
            else:
                yield from _recurse(a, mid)
                yield from _recurse(mid + dt.timedelta(days=1), b)
    yield from _recurse(start, (end - dt.timedelta(days=1)))

def main():
    ap = argparse.ArgumentParser(description="Bucket contributors by commits and merged PRs, with adaptive historical search.")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--until", required=True, help="YYYY-MM-DD exclusive")
    ap.add_argument("--token", required=True, help="GitHub PAT")
    ap.add_argument("--slice-months", type=int, default=1, help="Months per slice for window scanning")
    ap.add_argument("--historical-start", default="2014-01-01", help="Start date to scan history for Dormant/Newcomer detection")
    args = ap.parse_args()

    start = dt.datetime.fromisoformat(args.since)
    end = dt.datetime.fromisoformat(args.until)
    hist_start = dt.datetime.fromisoformat(args.historical_start)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/vnd.github+json", "Authorization": f"Bearer {args.token}"})

    #Window metrics
    commits_by = defaultdict(int)
    merged_prs_by = defaultdict(int)

    slices = month_slices(start.date(), end.date(), args.slice_months)
    if tqdm: tqdm.write(f"slices in window: {len(slices)}")

    #Commits in window
    if tqdm: p1 = tqdm(total=len(slices), desc="Commits", unit="slice", leave=False)
    for s, e in slices:
        url = COMMITS_URL_TMPL.format(owner=args.owner, repo=args.repo)
        params = {"since": to_iso(s), "until": to_iso(e)}
        for c in rest_paginated(sess, url, params, f"commits {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            user = c.get("author") or {}
            login = user.get("login")
            if login:
                commits_by[login] += 1
        if tqdm: p1.update(1)
    if tqdm: p1.close()

    #Merged PRs in window
    if tqdm: p2 = tqdm(total=len(slices), desc="Merged PRs", unit="slice", leave=False)
    for s, e in slices:
        base = f"repo:{args.owner}/{args.repo} type:pr is:merged"
        for it in adaptive_search_range(sess, base, s.date(), e.date(), "merged PRs"):
            user = it.get("user") or {}
            login = user.get("login")
            if login:
                merged_prs_by[login] += 1
        if tqdm: p2.update(1)
    if tqdm: p2.close()

    active_users = set(commits_by.keys()) | set(merged_prs_by.keys())

    #Historical (before window) for newcomer/dormant
    if tqdm: tqdm.write("Scanning historical activity (before window) with adaptive splitting…")
    hist_commit_authors: Set[str] = set()
    hist_slices = month_slices(hist_start.date(), start.date(), 6)
    if tqdm: p3 = tqdm(total=len(hist_slices), desc="Historical commits", unit="slice", leave=False)
    for s, e in hist_slices:
        url = COMMITS_URL_TMPL.format(owner=args.owner, repo=args.repo)
        params = {"since": to_iso(s), "until": to_iso(e)}
        for c in rest_paginated(sess, url, params, f"hist commits {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            user = c.get("author") or {}
            login = user.get("login")
            if login:
                hist_commit_authors.add(login)
        if tqdm: p3.update(1)
    if tqdm: p3.close()

    hist_pr_authors: Set[str] = set()
    base_hist = f"repo:{args.owner}/{args.repo} type:pr"
    for it in adaptive_search_range(sess, base_hist, hist_start.date(), start.date(), "hist PRs"):
        user = it.get("user") or {}
        login = user.get("login")
        if login:
            hist_pr_authors.add(login)

    historical_before_since = hist_commit_authors | hist_pr_authors

    #Classification
    bucket_counts = Counter()
    rows = []

    for login in sorted(active_users):
        c = commits_by.get(login, 0)
        m = merged_prs_by.get(login, 0)
        if login not in historical_before_since:
            bucket = "Newcomer"
        else:
            if c >= 200 or m >= 150:
                bucket = "Key"
            elif (20 <= c <= 199) or (25 <= m <= 149):
                bucket = "Frequent"
            elif (2 <= c <= 19) or (1 <= m <= 24):
                bucket = "Occasional"
            else:
                bucket = "Inactive"
        if bucket == "Inactive":
            bucket = "Occasional" if (c > 0 or m > 0) else "Inactive"
        bucket_counts[bucket] += 1
        rows.append((login, c, m, bucket))

    dormant_users = sorted(historical_before_since - active_users)
    bucket_counts["Dormant"] += len(dormant_users)

    order = ["Key", "Frequent", "Occasional", "Newcomer", "Dormant"]
    print("\n=== Contributor Buckets (exact, commits + merged PRs) ===")
    for b in order:
        print(f"{b:10s}: {bucket_counts.get(b, 0)}")

    print(f"\nActive unique (window): {len(active_users)}")
    print(f"Historical unique (< since): {len(historical_before_since)}")
    print(f"Total unique (overall): {len(active_users | historical_before_since)}")

    out = f"buckets_{args.owner}_{args.repo}_{args.since}_{args.until}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["login", "commits_in_window", "merged_prs_in_window", "bucket"])
        for login, c, m, bucket in rows:
            w.writerow([login, c, m, bucket])
        w.writerow([])
        w.writerow(["[Dormant users]"])
        for d in dormant_users:
            w.writerow([d, 0, 0, "Dormant"])

    print(f"\nCSV written: {out}")
    if tqdm is None:
        print("(Tip) Install tqdm for nicer progress bars:  pip install tqdm")

if __name__ == "__main__":
    main()
