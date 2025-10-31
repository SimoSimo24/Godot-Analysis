
import argparse
import csv
import datetime as dt
import os
import re
import sys
import time
import random
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

BOT_REGEXES = [
    re.compile(r".*\[bot\]$", re.IGNORECASE),
    re.compile(r".*-bot$", re.IGNORECASE),
    re.compile(r"^(dependabot|renovate|github-actions|travis-ci|circleci|appveyor|buildkite|azure-pipelines|codecov|sonarcloud|coveralls|bors|mergify)$", re.IGNORECASE),
]

NOREPLY_RE = re.compile(r"(?P<id>\d+)\+(?P<login>[A-Za-z0-9-]+)@users\.noreply\.github\.com$", re.IGNORECASE)

SPINNER_FRAMES = ["|", "/", "-", "\\"]
def spinner(i: int) -> str:
    return SPINNER_FRAMES[i % len(SPINNER_FRAMES)]

def is_bot_login(login: str) -> bool:
    if not login:
        return False
    for rx in BOT_REGEXES:
        if rx.match(login):
            return True
    return False

def iso_month_slices(start: dt.date, end: dt.date, months_per_slice: int = 1) -> List[Tuple[dt.datetime, dt.datetime]]:
    """Return [ (slice_start_iso, slice_end_iso) ... ) ] where end is exclusive."""
    slices = []
    cur = dt.date(start.year, start.month, 1)
    end_first = dt.date(end.year, end.month, 1)
    while cur < end_first:
        m = cur.month - 1 + months_per_slice
        y = cur.year + m // 12
        m = m % 12 + 1
        nxt = dt.date(y, m, 1)
        if nxt > end_first:
            nxt = end_first
        slices.append((dt.datetime.combine(cur, dt.time.min), dt.datetime.combine(nxt, dt.time.min)))
        cur = nxt
    return slices

class GH:
    def __init__(self, token: str, sleep: float = 0.3, max_retries: int = 6):
        self.sess = requests.Session()
        if token:
            self.sess.headers.update({"Authorization": f"Bearer {token}"})
        self.sess.headers.update({"Accept": "application/vnd.github+json"})
        self.sleep = sleep
        self.max_retries = max_retries

    def _request_with_retries(self, method: str, url: str, params: Optional[dict] = None) -> requests.Response:
        attempt = 0
        while True:
            attempt += 1
            resp = self.sess.request(method, url, params=params)
            #Handle rate limits explicitly
            if resp.status_code == 403 and "rate limit" in resp.text.lower():
                reset = int(resp.headers.get("X-RateLimit-Reset", "0"))
                now = int(time.time())
                wait = max(5, reset - now + 1)
                msg = f"Rate limited. Sleeping {wait}s..."
                if tqdm:
                    tqdm.write(msg)
                else:
                    print(msg, flush=True)
                time.sleep(wait)
                continue
            #Retry on transient 5xx
            if resp.status_code >= 500 and attempt <= self.max_retries:
                backoff = min(30, (2 ** (attempt - 1))) + random.uniform(0, 1.5)
                msg = f"Server error {resp.status_code} on {url}. Retry {attempt}/{self.max_retries} in {backoff:.1f}s"
                if tqdm:
                    tqdm.write(msg)
                else:
                    print(msg, flush=True)
                time.sleep(backoff)
                continue
            if resp.status_code >= 400 and resp.status_code < 500:
                # For 4xx not rate-limit, just raise
                raise RuntimeError(f"GitHub API error {resp.status_code}: {resp.text[:300]}")
            #Success
            time.sleep(self.sleep)
            return resp

    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        return self._request_with_retries("GET", url, params=params)

    def _paginate(self, url: str, params: dict, desc: str = "") -> Iterable[dict]:
        page = 1
        i = 0
        while True:
            params["per_page"] = 100
            params["page"] = page
            r = self._get(url, params=params)
            data = r.json()
            if not isinstance(data, list):
                raise RuntimeError(f"Expected list from {url}, got: {type(data)} - {data}")
            if not data:
                break
            for item in data:
                i += 1
                if tqdm is None and i % 200 == 0:
                    print(f"  {desc}: fetched {i} items...", flush=True)
                yield item
            if 'next' not in r.links:
                break
            page += 1

    def commits(self, owner: str, repo: str, since_iso: str, until_iso: str) -> Iterable[dict]:
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        params = {"since": since_iso, "until": until_iso}
        yield from self._paginate(url, params, desc="commits")

    def issues_comments(self, owner: str, repo: str, since_iso: str) -> Iterable[dict]:
        url = f"https://api.github.com/repos/{owner}/{repo}/issues/comments"
        params = {"since": since_iso}
        yield from self._paginate(url, params, desc="issue_comments")

    def review_comments(self, owner: str, repo: str, since_iso: str) -> Iterable[dict]:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/comments"
        params = {"since": since_iso}
        yield from self._paginate(url, params, desc="review_comments")

    def search_issues(self, q: str, desc: str) -> Iterable[dict]:
        url = "https://api.github.com/search/issues"
        page = 1
        i = 0
        while True:
            params = {"q": q, "per_page": 100, "page": page}
            r = self._get(url, params=params)
            j = r.json()
            items = j.get("items", [])
            for it in items:
                i += 1
                if tqdm is None and i % 200 == 0:
                    print(f"  search {desc}: fetched {i} items...", flush=True)
                yield it
            if len(items) < 100:
                break
            page += 1

def parse_args():
    ap = argparse.ArgumentParser(description="Exact unique human contributors with resilient progress.")
    ap.add_argument("--owner", required=True, help="Repository owner/org, e.g. godotengine")
    ap.add_argument("--repo", required=True, help="Repository name, e.g. godot")
    ap.add_argument("--since", required=True, help="Start date (inclusive), YYYY-MM-DD")
    ap.add_argument("--until", required=True, help="End date (exclusive), YYYY-MM-DD")
    ap.add_argument("--token", required=True, help="GitHub Personal Access Token")
    ap.add_argument("--slice-months", type=int, default=1, help="Months per slice (default 1)")
    ap.add_argument("--include-reviews", action="store_true", help="Also include PR reviewers (extra API calls).")
    ap.add_argument("--skip-issue-comments", action="store_true", help="Skip fetching issue comments (faster/safer).")
    ap.add_argument("--skip-review-comments", action="store_true", help="Skip fetching PR review comments (faster/safer).")
    ap.add_argument("--max-retries", type=int, default=6, help="Max retries on 5xx errors (default 6).")
    return ap.parse_args()

def to_iso(dt_obj: dt.datetime) -> str:
    return dt_obj.isoformat() + "Z"

def is_in_window(iso_str: str, start: dt.datetime, end: dt.datetime) -> bool:
    try:
        t = dt.datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)
        return start <= t < end
    except Exception:
        return False

def collect_contributors(owner: str, repo: str, start: dt.datetime, end: dt.datetime, token: str, slice_months: int, include_reviews: bool, skip_issue_comments: bool, skip_review_comments: bool, max_retries: int):
    gh = GH(token, max_retries=max_retries)

    human_logins: Set[str] = set()
    bot_logins: Set[str] = set()
    email_to_logins: Dict[str, Set[str]] = {}
    contributions: Dict[str, Set[str]] = {}

    def add(login: Optional[str], typ: str):
        if not login:
            return
        if is_bot_login(login):
            bot_logins.add(login)
            return
        human_logins.add(login)
        contributions.setdefault(login, set()).add(typ)

    slices = iso_month_slices(start.date(), end.date(), months_per_slice=slice_months)
    if tqdm:
        tqdm.write(f"Monthly slices: {len(slices)}")

    #Commits
    if tqdm:
        pbar = tqdm(total=len(slices), desc="Commits (per slice)", unit="slice", leave=False)
    for s, e in slices:
        since_iso = to_iso(s)
        until_iso = to_iso(e)
        count = 0
        for c in gh.commits(owner, repo, since_iso, until_iso):
            user = c.get("author") or {}
            login = user.get("login")
            add(login, "commit")
            commit_author = c.get("commit", {}).get("author", {}) or {}
            email = (commit_author.get("email") or "").strip().lower()
            if email:
                m = NOREPLY_RE.search(email)
                if m:
                    login_from_email = m.group("login")
                    if login_from_email:
                        add(login_from_email, "commit")
                        login = login or login_from_email
                email_to_logins.setdefault(email, set()).add(login or f"unknown:{email}")
            count += 1
            if not tqdm and count % 200 == 0:
                print(f"[{s.date()}..{(e - dt.timedelta(days=1)).date()}] commits fetched: {count} {spinner(count)}", end="\r", flush=True)
        if not tqdm:
            print(f"[{s.date()}..{(e - dt.timedelta(days=1)).date()}] commits fetched: {count}            ")
        else:
            pbar.update(1)
    if tqdm:
        pbar.close()

    #PRs
    if tqdm:
        pbar = tqdm(total=len(slices), desc="Pull requests (per slice)", unit="slice", leave=False)
    for s, e in slices:
        q = f'repo:{owner}/{repo} type:pr created:{s.isoformat()}..{(e - dt.timedelta(days=1)).isoformat()}'
        count = 0
        for it in gh.search_issues(q, desc=f"PRs {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            user = it.get("user") or {}
            add(user.get("login"), "pr")
            count += 1
        if not tqdm:
            print(f"[{s.date()}..{(e - dt.timedelta(days=1)).date()}] PRs fetched: {count}")
        else:
            pbar.update(1)
    if tqdm:
        pbar.close()

    #Issue authrors
    if tqdm:
        pbar = tqdm(total=len(slices), desc="Issues (per slice)", unit="slice", leave=False)
    for s, e in slices:
        q = f'repo:{owner}/{repo} type:issue created:{s.isoformat()}..{(e - dt.timedelta(days=1)).isoformat()}'
        count = 0
        for it in gh.search_issues(q, desc=f"Issues {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            user = it.get("user") or {}
            add(user.get("login"), "issue")
            count += 1
        if not tqdm:
            print(f"[{s.date()}..{(e - dt.timedelta(days=1)).date()}] issues fetched: {count}")
        else:
            pbar.update(1)
    if tqdm:
        pbar.close()

    #Issue comments (optional)
    if not skip_issue_comments:
        if tqdm:
            pbar_ic = tqdm(total=0, desc="Issue comments (stream)", unit="item", leave=False)
        ic_count = 0
        last_tick = time.time()
        try:
            for c in gh.issues_comments(owner, repo, to_iso(start)):
                if is_in_window(c.get("created_at", ""), start, end):
                    user = c.get("user") or {}
                    add(user.get("login"), "issue_comment")
                    ic_count += 1
                    if tqdm:
                        if time.time() - last_tick > 0.5:
                            pbar_ic.set_description(f"Issue comments (count={ic_count})")
                            last_tick = time.time()
                    elif ic_count % 200 == 0:
                        print(f"Issue comments fetched: {ic_count} {spinner(ic_count)}", end="\r", flush=True)
        except RuntimeError as e:
            msg = f"[WARN] Issue comments endpoint failed persistently and will be skipped: {e}"
            if tqdm:
                tqdm.write(msg)
            else:
                print(msg, flush=True)
        if not tqdm:
            print(f"Issue comments fetched: {ic_count}            ")
        else:
            pbar_ic.close()

    #Review comments (optional)
    if not skip_review_comments:
        if tqdm:
            pbar_rc = tqdm(total=0, desc="PR review comments (stream)", unit="item", leave=False)
        rc_count = 0
        last_tick = time.time()
        try:
            for c in gh.review_comments(owner, repo, to_iso(start)):
                if is_in_window(c.get("created_at", ""), start, end):
                    user = c.get("user") or {}
                    add(user.get("login"), "review_comment")
                    rc_count += 1
                    if tqdm:
                        if time.time() - last_tick > 0.5:
                            pbar_rc.set_description(f"PR review comments (count={rc_count})")
                            last_tick = time.time()
                    elif rc_count % 200 == 0:
                        print(f"PR review comments fetched: {rc_count} {spinner(rc_count)}", end="\r", flush=True)
        except RuntimeError as e:
            msg = f"[WARN] PR review comments endpoint failed persistently and will be skipped: {e}"
            if tqdm:
                tqdm.write(msg)
            else:
                print(msg, flush=True)
        if not tqdm:
            print(f"PR review comments fetched: {rc_count}            ")
        else:
            pbar_rc.close()

    #PR reviews (optional)
    if include_reviews:
        for s, e in slices:
            if tqdm:
                pbar = tqdm(total=0, desc=f"PR reviews {s.date()}..{(e - dt.timedelta(days=1)).date()} (stream)", leave=False)
            count = 0
            q = f'repo:{owner}/{repo} type:pr created:{s.isoformat()}..{(e - dt.timedelta(days=1)).isoformat()}'
            try:
                for it in gh.search_issues(q, desc=f"PRs for reviews {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
                    number = it.get("number")
                    if number:
                        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}/reviews"
                        page = 1
                        while True:
                            params = {"per_page": 100, "page": page}
                            r = gh._get(url, params=params)
                            reviews = r.json()
                            if not isinstance(reviews, list) or not reviews:
                                break
                            for rv in reviews:
                                submitted_at = rv.get("submitted_at")
                                if submitted_at and is_in_window(submitted_at, start, end):
                                    user = rv.get("user") or {}
                                    add(user.get("login"), "review")
                                    count += 1
                                    if tqdm:
                                        pbar.set_description(f"PR reviews {s.date()}..{(e - dt.timedelta(days=1)).date()} (count={count})")
                                    elif count % 200 == 0:
                                        print(f"PR reviews fetched so far: {count} {spinner(count)}", end="\r", flush=True)
                            if 'next' not in r.links:
                                break
                            page += 1
            except RuntimeError as err:
                msg = f"[WARN] PR reviews endpoint failed persistently for slice {s.date()}..{(e - dt.timedelta(days=1)).date()} and will be skipped: {err}"
                if tqdm:
                    tqdm.write(msg)
                else:
                    print(msg, flush=True)
            if not tqdm:
                print(f"PR reviews {s.date()}..{(e - dt.timedelta(days=1)).date()} fetched: {count}")
            else:
                pbar.close()

    dedupe_groups = []
    for email, logins in email_to_logins.items():
        if NOREPLY_RE.search(email):
            continue
        if len(logins) > 1:
            dedupe_groups.append((email, sorted(logins)))

    alias_map: Dict[str, str] = {}
    for email, group_logins in dedupe_groups:
        canonical = group_logins[0]
        for lg in group_logins[1:]:
            alias_map[lg] = canonical

    final_logins: Set[str] = set()
    for lg in human_logins:
        canonical = alias_map.get(lg, lg)
        final_logins.add(canonical)

    return {
        "raw_human_accounts": sorted(human_logins),
        "bot_accounts": sorted(bot_logins),
        "final_unique_humans": sorted(final_logins),
        "contributions": contributions,
        "dedupe_groups": dedupe_groups,
    }

def main():
    args = parse_args()
    start = dt.datetime.fromisoformat(args.since)
    end = dt.datetime.fromisoformat(args.until)

    if tqdm:
        tqdm.write(f"Repository: {args.owner}/{args.repo}")
        tqdm.write(f"Window:    [{args.since} .. {args.until})")
    else:
        print(f"Repository: {args.owner}/{args.repo}")
        print(f"Window:    [{args.since} .. {args.until})")

    res = collect_contributors(
        owner=args.owner,
        repo=args.repo,
        start=start,
        end=end,
        token=args.token,
        slice_months=args.slice_months,
        include_reviews=args.include_reviews,
        skip_issue_comments=args.skip_issue_comments,
        skip_review_comments=args.skip_review_comments,
        max_retries=args.max_retries,
    )

    raw_humans = res["raw_human_accounts"]
    bots = res["bot_accounts"]
    finals = res["final_unique_humans"]
    contributions = res["contributions"]
    dedupe_groups = res["dedupe_groups"]

    print("\n=== EXACT CONTRIBUTOR COUNTS ===")
    print(f"Human accounts (pre-dedupe): {len(raw_humans)}")
    print(f"Bot accounts excluded:        {len(bots)}")
    print(f"Email-based dedupe groups:    {len(dedupe_groups)}")
    print(f"UNIQUE HUMAN CONTRIBUTORS:    {len(finals)}")

    #Write audit CSV
    fname = f"contributors_{args.owner}_{args.repo}_{args.since}_{args.until}.csv"
    path = os.path.join(os.getcwd(), fname)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["login", "is_bot_filtered", "deduped_to", "contribution_types"])
        alias_map = {}
        for email, group in dedupe_groups:
            canonical = group[0]
            for lg in group[1:]:
                alias_map[lg] = canonical
        for lg in sorted(set(list(raw_humans) + list(bots))):
            is_bot = lg in bots
            dedup_to = alias_map.get(lg, "")
            kinds = ",".join(sorted(contributions.get(lg, [])))
            w.writerow([lg, "yes" if is_bot else "no", dedup_to, kinds])

    print(f"\nAudit CSV written: {path}")
    if tqdm is None:
        print("(Tip) Install tqdm for nicer progress bars:  pip install tqdm")

if __name__ == "__main__":
    main()
