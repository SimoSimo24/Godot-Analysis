
import argparse
import csv
import datetime as dt
import math
import statistics as stats
from collections import defaultdict, Counter
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union, Mapping
import time
import random

import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

COMMITS_URL_TMPL = "https://api.github.com/repos/{owner}/{repo}/commits"
ISSUE_COMMENTS_URL_TMPL = "https://api.github.com/repos/{owner}/{repo}/issues/comments"
PR_REVIEW_COMMENTS_URL_TMPL = "https://api.github.com/repos/{owner}/{repo}/pulls/comments"
SEARCH_URL = "https://api.github.com/search/issues"
USER_URL_TMPL = "https://api.github.com/users/{login}"

def to_iso(d: dt.datetime) -> str:
    return d.isoformat() + "Z"

def parse_iso(ts: str) -> Optional[dt.datetime]:
    if not ts: return None
    try:
        return dt.datetime.fromisoformat(ts.replace("Z","+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def month_slices(start: dt.date, end: dt.date, months_per_slice: int = 1):
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

def search_items(sess: requests.Session, q: str, desc: str) -> Iterable[dict]:
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
        if len(items) < 100 or page >= 10:
            break
        page += 1

def classify_bot_without_username(sess: requests.Session, login: str, profile_cache: Dict[str, dict], timestamps: List[dt.datetime], via_app_hits: int) -> Tuple[bool, Dict[str, Union[str, bool]]]:
    reasons: Dict[str, Union[str, bool]] = {}
    if login not in profile_cache:
        r = sess.get(USER_URL_TMPL.format(login=login))
        if r.status_code == 403 and "rate limit" in r.text.lower():
            reset = int(r.headers.get("X-RateLimit-Reset", "0"))
            now = int(time.time())
            wait = max(5, reset - now + 1)
            (tqdm.write if tqdm else print)(f"[rate limit] sleeping {wait}s (profile {login})")
            time.sleep(wait)
            r = sess.get(USER_URL_TMPL.format(login=login))
        r.raise_for_status()
        profile_cache[login] = r.json()
    prof = profile_cache[login]
    if prof.get("type") == "Bot":
        reasons["type"] = "GitHub marks account as Bot"
        reasons["is_bot"] = True
        return True, reasons

    if via_app_hits > 0:
        reasons["github_app"] = f"{via_app_hits} actions performed via GitHub App"

    ts_sorted = sorted([t for t in timestamps if t is not None])
    temporal_flag = False
    if len(ts_sorted) >= 50:
        hours = Counter(t.hour for t in ts_sorted)
        active_hours = sum(1 for h,c in hours.items() if c>0)
        night = sum(c for h,c in hours.items() if 0 <= h <= 5)
        share_night = night / len(ts_sorted)
        gaps = [(ts_sorted[i]-ts_sorted[i-1]).total_seconds() for i in range(1, len(ts_sorted))]
        if gaps:
            mean_gap = sum(gaps)/len(gaps)
            std_gap = (sum((g-mean_gap)**2 for g in gaps)/len(gaps))**0.5
            cv = (std_gap/mean_gap) if mean_gap>0 else 0.0
        else:
            mean_gap = 0.0; cv = 0.0
        if active_hours >= 20 and share_night >= 0.4 and mean_gap <= 600 and cv <= 0.6:
            temporal_flag = True
            reasons["temporal"] = f"24/7-like activity (active_hours={active_hours}, night_share={share_night:.2f}, mean_gap={mean_gap:.0f}s, cv={cv:.2f})"

    followers = prof.get("followers", 0) or 0
    following = prof.get("following", 0) or 0
    created_at = prof.get("created_at")
    if created_at:
        try:
            created = dt.datetime.fromisoformat(created_at.replace("Z","+00:00")).replace(tzinfo=None)
            age_days = (dt.datetime.utcnow() - created).days
        except Exception:
            age_days = 9999
    else:
        age_days = 9999
    volume = len(ts_sorted)
    minimal_profile_flag = (followers==0 and following==0 and age_days < 90 and volume >= 50)
    if minimal_profile_flag:
        reasons["profile"] = f"new/minimal profile (age_days={age_days}, followers={followers}, following={following}, volume={volume})"

    is_bot = False
    if prof.get("type") == "Bot":
        is_bot = True
    elif via_app_hits > 0 and (temporal_flag or minimal_profile_flag):
        is_bot = True
    else:
        is_bot = False

    return is_bot, reasons

def main():
    ap = argparse.ArgumentParser(description="Flag likely bots without using username patterns.")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--until", required=True, help="YYYY-MM-DD exclusive")
    ap.add_argument("--token", required=True, help="GitHub PAT")
    ap.add_argument("--slice-months", type=int, default=1)
    ap.add_argument("--include-issue-comments", action="store_true")
    ap.add_argument("--include-review-comments", action="store_true")
    args = ap.parse_args()

    start = dt.datetime.fromisoformat(args.since)
    end = dt.datetime.fromisoformat(args.until)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/vnd.github+json", "Authorization": f"Bearer {args.token}"})

    timestamps_by: Dict[str, List[Optional[dt.datetime]]] = defaultdict(list)
    via_app_hits_by: Dict[str, int] = defaultdict(int)

    slices = month_slices(start.date(), end.date(), args.slice_months)
    if tqdm: tqdm.write(f"slices: {len(slices)}")

    #Commits
    if tqdm: p1 = tqdm(total=len(slices), desc="Commits", unit="slice", leave=False)
    for s, e in slices:
        url = COMMITS_URL_TMPL.format(owner=args.owner, repo=args.repo)
        params = {"since": to_iso(s), "until": to_iso(e)}
        for c in rest_paginated(sess, url, params, f"commits {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            user = c.get("author") or {}
            login = user.get("login")
            t = None
            ca = c.get("commit",{}).get("author",{})
            if ca: t = parse_iso(ca.get("date",""))
            if login:
                timestamps_by[login].append(t)
            committer = c.get("committer") or {}
            if committer.get("type") == "Bot" and committer.get("login"):
                timestamps_by[committer["login"]].append(t)
        if tqdm: p1.update(1)
    if tqdm: p1.close()

    #PR authors
    if tqdm: p2 = tqdm(total=len(slices), desc="PR authors", unit="slice", leave=False)
    for s, e in slices:
        q = f'repo:{args.owner}/{args.repo} type:pr created:{s.date()}..{(e - dt.timedelta(days=1)).date()}'
        for it in search_items(sess, q, f"prs {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            user = it.get("user") or {}
            login = user.get("login")
            t = parse_iso(it.get("created_at",""))
            if login:
                timestamps_by[login].append(t)
        if tqdm: p2.update(1)
    if tqdm: p2.close()

    #Optional comments
    if args.include_issue_comments:
        if tqdm: p3 = tqdm(total=1, desc="Issue comments", unit="req", leave=False)
        url = ISSUE_COMMENTS_URL_TMPL.format(owner=args.owner, repo=args.repo)
        params = {"since": to_iso(start)}
        for it in rest_paginated(sess, url, params, "issue_comments"):
            t = parse_iso(it.get("created_at",""))
            user = it.get("user") or {}
            login = user.get("login")
            if login:
                timestamps_by[login].append(t)
                if it.get("performed_via_github_app"):
                    via_app_hits_by[login] += 1
        if tqdm: p3.update(1); p3.close()

    if args.include_review_comments:
        if tqdm: p4 = tqdm(total=1, desc="PR review comments", unit="req", leave=False)
        url = PR_REVIEW_COMMENTS_URL_TMPL.format(owner=args.owner, repo=args.repo)
        params = {"since": to_iso(start)}
        for it in rest_paginated(sess, url, params, "review_comments"):
            t = parse_iso(it.get("created_at",""))
            user = it.get("user") or {}
            login = user.get("login")
            if login:
                timestamps_by[login].append(t)
                if it.get("performed_via_github_app"):
                    via_app_hits_by[login] += 1
        if tqdm: p4.update(1); p4.close()

    #Classify
    profile_cache: Dict[str, dict] = {}
    decisions: Dict[str, Dict[str, Union[str, bool]]] = {}
    all_logins = sorted(timestamps_by.keys())
    if tqdm: bar = tqdm(total=len(all_logins), desc="Classifying", unit="acct", leave=False)
    for login in all_logins:
        ts = [t for t in timestamps_by[login] if t is not None]
        via_app_hits = via_app_hits_by.get(login, 0)
        is_bot, reasons = classify_bot_without_username(sess, login, profile_cache, ts, via_app_hits)
        reasons_out: Dict[str, Union[str, bool]] = dict(reasons)  # Create a new dict with the same key/value pairs
        reasons_out["is_bot"] = is_bot
        decisions[login] = reasons_out
        if tqdm: bar.update(1)
    if tqdm: bar.close()

    num_bots = sum(1 for d in decisions.values() if d["is_bot"])
    num_humans = len(decisions) - num_bots
    print("\n=== Bot filter (no username patterns) ===")
    print(f"Active accounts in window: {len(decisions)}")
    print(f"Likely bots: {num_bots}")
    print(f"Likely humans: {num_humans}")

    out = f"bots_nousername_{args.owner}_{args.repo}_{args.since}_{args.until}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["login","is_bot","reasons"])
        for login in all_logins:
            d = decisions[login]
            reasons_str = "; ".join([f"{k}={v}" for k,v in d.items() if k!="is_bot"])
            w.writerow([login, "yes" if d["is_bot"] else "no", reasons_str])
    print(f"CSV written: {out}")
    if tqdm is None:
        print("(Tip) Install tqdm for nicer progress bars:  pip install tqdm")

if __name__ == "__main__":
    main()
