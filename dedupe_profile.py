
import argparse
import csv
import datetime as dt
import time, random
import sys
from typing import List, Dict, Optional, Tuple, Set
import requests

# same name (+2), same blog/company (+1 each), created within 1 day (+1) -> score>=3 = suspicious

def main():
    ap = argparse.ArgumentParser(description="Detect suspicious duplicate accounts by profile similarity.")
    ap.add_argument("--token", required=True)
    ap.add_argument("--input-logins", help="CSV with 'login' column. If omitted, you must pass --owner/--repo/--since/--until.")
    ap.add_argument("--owner")
    ap.add_argument("--repo")
    ap.add_argument("--since")
    ap.add_argument("--until")
    args = ap.parse_args()

    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {args.token}", "Accept": "application/vnd.github+json"})

    def gh_get(url, params=None, desc=""):
        while True:
            r = sess.get(url, params=params)
            if r.status_code == 403 and "rate limit" in r.text.lower():
                reset = int(r.headers.get("X-RateLimit-Reset","0"))
                now = int(time.time())
                wait = max(5, reset-now+1)
                print(f"[rate limit] {desc} sleeping {wait}s")
                time.sleep(wait); continue
            if r.status_code >= 500:
                back = min(30, 2**random.randint(0,4)) + random.uniform(0,1.0)
                print(f"[server] {r.status_code} on {desc}, retry in {back:.1f}s")
                time.sleep(back); continue
            r.raise_for_status()
            return r

    logins: List[str] = []
    if args.input_logins:
        with open(args.input_logins, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                lg = (row.get("login") or "").strip()
                if lg:
                    logins.append(lg)
    else:
        if not (args.owner and args.repo and args.since and args.until):
            print("Need --input-logins OR (--owner --repo --since --until)")
            sys.exit(1)
        start = dt.datetime.fromisoformat(args.since)
        end   = dt.datetime.fromisoformat(args.until)
        def to_iso(d: dt.datetime) -> str: return d.isoformat() + "Z"
        def month_slices(st: dt.date, ed: dt.date):
            cur = dt.date(st.year, st.month, 1)
            end1 = dt.date(ed.year, ed.month, 1)
            while cur < end1:
                if cur.month == 12:
                    nxt = dt.date(cur.year+1, 1, 1)
                else:
                    nxt = dt.date(cur.year, cur.month+1, 1)
                yield dt.datetime.combine(cur, dt.time.min), dt.datetime.combine(min(nxt, end1), dt.time.min)
                cur = nxt
        for s,e in month_slices(start.date(), end.date()):
            #commits
            r = gh_get(f"https://api.github.com/repos/{args.owner}/{args.repo}/commits",
                       {"since": to_iso(s), "until": to_iso(e), "per_page": 100},
                       desc="commits")
            for c in r.json():
                u = (c.get("author") or {}).get("login")
                if u: logins.append(u)
            #PRs
            search_q = f"repo:{args.owner}/{args.repo} type:pr created:{s.date()}..{(e - dt.timedelta(days=1)).date()}"
            r = gh_get("https://api.github.com/search/issues",
                       {"q": search_q, "per_page": 100},
                       desc="prs")
            for it in r.json().get("items", []):
                u = (it.get("user") or {}).get("login")
                if u: logins.append(u)

    logins = sorted(set(logins))
    print(f"collected {len(logins)} unique logins")

    #fetch profiles
    profiles: Dict[str, dict] = {}
    for lg in logins:
        r = gh_get(f"https://api.github.com/users/{lg}", desc=f"user/{lg}")
        profiles[lg] = r.json()

    def norm(s: Optional[str]) -> str:
        return (s or "").strip().lower()

    susp: List[Tuple[str,str,int]] = []
    n = len(logins)
    for i in range(n):
        a = logins[i]
        pa = profiles[a]
        for j in range(i+1, n):
            b = logins[j]
            pb = profiles[b]
            score = 0
            if norm(pa.get("name")) and norm(pa.get("name")) == norm(pb.get("name")):
                score += 2
            if norm(pa.get("blog")) and norm(pa.get("blog")) == norm(pb.get("blog")):
                score += 1
            if norm(pa.get("company")) and norm(pa.get("company")) == norm(pb.get("company")):
                score += 1
            try:
                ca = dt.datetime.fromisoformat(pa["created_at"].replace("Z","+00:00"))
                cb = dt.datetime.fromisoformat(pb["created_at"].replace("Z","+00:00"))
                if abs((ca - cb).total_seconds()) < 86400:
                    score += 1
            except Exception:
                pass
            if score >= 3:
                susp.append((a,b,score))

    out = "dedupe_by_profile_suspicious.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["login_a","login_b","score"])
        for a,b,s in susp:
            w.writerow([a,b,s])
    print(f"suspicious pairs: {len(susp)}")
    print(f"CSV written: {out}")

if __name__ == "__main__":
    main()
