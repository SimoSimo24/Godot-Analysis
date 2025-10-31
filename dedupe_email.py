
import argparse
import csv
import datetime as dt
import time
import random
import re
from typing import Dict, Set, Optional, Iterable, Tuple, List

import requests

NOREPLY_RE = re.compile(r"(?P<id>\d+)\+(?P<login>[A-Za-z0-9-]+)@users\.noreply\.github\.com$", re.IGNORECASE)

def month_slices(start: dt.date, end: dt.date, months: int = 1) -> List[Tuple[dt.datetime, dt.datetime]]:
    out = []
    cur = dt.date(start.year, start.month, 1)
    end1 = dt.date(end.year, end.month, 1)
    while cur < end1:
        m = cur.month - 1 + months
        y = cur.year + m // 12
        m = m % 12 + 1
        nxt = dt.date(y, m, 1)
        if nxt > end1:
            nxt = end1
        out.append((dt.datetime.combine(cur, dt.time.min), dt.datetime.combine(nxt, dt.time.min)))
        cur = nxt
    return out

class GH:
    def __init__(self, token: str, sleep: float = 0.3, max_retries: int = 5):
        self.sess = requests.Session()
        if token:
            self.sess.headers.update({"Authorization": f"Bearer {token}"})
        self.sess.headers.update({"Accept": "application/vnd.github+json"})
        self.sleep = sleep
        self.max_retries = max_retries

    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        tries = 0
        while True:
            tries += 1
            r = self.sess.get(url, params=params)
            if r.status_code == 403 and "rate limit" in r.text.lower():
                reset = int(r.headers.get("X-RateLimit-Reset", "0"))
                now = int(time.time())
                wait = max(5, reset - now + 1)
                print(f"[rate limit] sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code >= 500 and tries <= self.max_retries:
                back = min(30, 2**tries) + random.uniform(0,1.0)
                print(f"[server {r.status_code}] retry in {back:.1f}s")
                time.sleep(back)
                continue
            r.raise_for_status()
            time.sleep(self.sleep)
            return r

    def commits(self, owner: str, repo: str, since_iso: str, until_iso: str) -> Iterable[dict]:
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        page = 1
        params = {"since": since_iso, "until": until_iso, "per_page": 100}
        while True:
            params["page"] = page
            r = self._get(url, params)
            arr = r.json()
            if not arr:
                break
            for c in arr:
                yield c
            if "next" not in r.links:
                break
            page += 1

def to_iso(d: dt.datetime) -> str:
    return d.isoformat() + "Z"

def main():
    ap = argparse.ArgumentParser(description="Detect duplicate accounts by shared commit email.")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--slice-months", type=int, default=1)
    args = ap.parse_args()

    start = dt.datetime.fromisoformat(args.since)
    end   = dt.datetime.fromisoformat(args.until)

    gh = GH(args.token)

    email_to_logins: Dict[str, Set[str]] = {}

    slices = month_slices(start.date(), end.date(), args.slice_months)
    print(f"slices: {len(slices)}")

    for s, e in slices:
        print(f"slice {s.date()}..{(e - dt.timedelta(days=1)).date()}")
        for c in gh.commits(args.owner, args.repo, to_iso(s), to_iso(e)):
            author_login = (c.get("author") or {}).get("login")
            ca = c.get("commit", {}).get("author", {}) or {}
            email = (ca.get("email") or "").strip().lower()
            if not email:
                continue
            #Avoid calling the regex twice
            m = NOREPLY_RE.search(email)
            if m:
                login_from_email = m.group("login").lower()
                email_to_logins.setdefault(email, set()).add(login_from_email)
                if author_login and author_login.lower() != login_from_email:
                    email_to_logins[email].add(author_login.lower())
            else:
                email_to_logins.setdefault(email, set()).add((author_login or f"unknown:{email}").lower())

    #find duplicates
    groups = [(email, sorted(list(logins))) for email, logins in email_to_logins.items() if len(logins) > 1]

    print("\n=== duplicate accounts (by email) ===")
    for email, logins in groups:
        print(f"{email}: {', '.join(logins)}")

    out = f"dedupe_by_email_{args.owner}_{args.repo}_{args.since}_{args.until}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["email","login"])
        for email, logins in groups:
            for lg in logins:
                w.writerow([email, lg])
    print(f"CSV written: {out}")

if __name__ == "__main__":
    main()
