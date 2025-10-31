import argparse
import datetime as dt
import time, random, math, statistics as stats
from collections import defaultdict, Counter
from typing import Dict, Iterable, List, Set, Tuple
import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

SEARCH = "https://api.github.com/search/issues"
REPO   = "https://api.github.com/repos/{owner}/{repo}"
PULLS  = REPO + "/pulls"
ISSUES = REPO + "/issues"
ORG_MEMBERS = "https://api.github.com/orgs/{org}/members"

def z(d: dt.datetime) -> str:
    return d.isoformat() + "Z"

def iso(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z","+00:00")).replace(tzinfo=None)

def month_slices(start: dt.date, end: dt.date, months=1):
    cur = dt.date(start.year, start.month, 1)
    end1 = dt.date(end.year, end.month, 1)
    while cur < end1:
        m = cur.month - 1 + months
        y = cur.year + m // 12
        m = m % 12 + 1
        nxt = dt.date(y, m, 1)
        yield dt.datetime.combine(cur, dt.time.min), dt.datetime.combine(min(nxt, end1), dt.time.min)
        cur = nxt

def rate_sleep(resp, label=""):
    if resp.status_code == 403 and "rate limit" in resp.text.lower():
        reset = int(resp.headers.get("X-RateLimit-Reset", "0"))
        now   = int(time.time())
        wait  = max(5, reset - now + 1)
        (tqdm.write if tqdm else print)(f"[rate limit] {label} sleeping {wait}s")
        time.sleep(wait)
        return True
    return False

def get_json(sess, url, params=None, desc=""):
    while True:
        r = sess.get(url, params=params)
        if rate_sleep(r, desc):
            continue
        if r.status_code >= 500:
            back = min(30, 2**random.randint(0,4)) + random.uniform(0,1.0)
            (tqdm.write if tqdm else print)(f"[{desc}] {r.status_code}, retry in {back:.1f}s")
            time.sleep(back)
            continue
        r.raise_for_status()
        return r.json(), r.links

def paged(sess, url, params, desc=""):
    page = 1
    while True:
        params = dict(params or {}, per_page=100, page=page)
        j, links = get_json(sess, url, params, desc)
        if isinstance(j, dict):  
            items = j.get("items", [])
            for it in items:
                yield it
            if len(items) < 100 or page >= 10:
                break
        else:  
            if not j:
                break
            for it in j:
                yield it
            if "next" not in links:
                break
        page += 1

def load_org_members(sess, owner: str) -> Set[str]:
    members = set()
    try:
        for m in paged(sess, ORG_MEMBERS.format(org=owner), {}, "org_members"):
            lg = (m.get("login") or "").lower()
            if lg:
                members.add(lg)
    except requests.HTTPError:
        pass
    return members


def main():
    ap = argparse.ArgumentParser(description="Per-bucket contributor activity metrics")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since", required=True)     
    ap.add_argument("--until", required=True)       
    ap.add_argument("--token", required=True)
    ap.add_argument("--slice-months", type=int, default=1)
    ap.add_argument("--skip-review-latency", action="store_true",
                    help="Skip PR review latency collection (faster)")
    args = ap.parse_args()

    SINCE = dt.datetime.fromisoformat(args.since)
    UNTIL = dt.datetime.fromisoformat(args.until)

    sess = requests.Session()
    sess.headers.update({
        "Accept":"application/vnd.github+json",
        "Authorization":f"Bearer {args.token}",
        "User-Agent":"contrib-buckets/1.0"
    })

    #Maintainers
    maintainers = load_org_members(sess, args.owner)

    #collect commits per author in window
    commits_by = defaultdict(int)
    if tqdm: bar = tqdm(total=0, desc="Commits", unit="slice")
    for s, e in month_slices(SINCE.date(), UNTIL.date(), args.slice_months):
        for c in paged(sess,
                       (REPO + "/commits").format(owner=args.owner, repo=args.repo),
                       {"since": z(s), "until": z(e)},
                       f"commits {s.date()}..{(e - dt.timedelta(days=1)).date()}"):
            u = (c.get("author") or {}).get("login")
            if u:
                commits_by[u.lower()] += 1
        if tqdm: bar.update(1)
    if tqdm: bar.close()

    #collect merged PRs per author in window
    merged_prs_by = defaultdict(int)
    pr_numbers_by_author = defaultdict(list)
    if tqdm: bar = tqdm(total=0, desc="Merged PRs", unit="slice")
    for s, e in month_slices(SINCE.date(), UNTIL.date(), args.slice_months):
        start_d = s.date()
        end_d   = (e - dt.timedelta(days=1)).date()
        q = f"repo:{args.owner}/{args.repo} type:pr is:merged created:{start_d}..{end_d}"
        for it in paged(sess, SEARCH, {"q": q}, f"merged PRs {start_d}..{end_d}"):
            user = (it.get("user") or {}).get("login")
            num  = it.get("number")
            if user and num:
                merged_prs_by[user.lower()] += 1
                pr_numbers_by_author[user.lower()].append(num)
        if tqdm: bar.update(1)
    if tqdm: bar.close()

    #collect issues closed per user in window
    issues_closed_by = defaultdict(int)
    if tqdm: bar = tqdm(total=0, desc="Closed issues", unit="slice")
    for s, e in month_slices(SINCE.date(), UNTIL.date(), args.slice_months):
        start_d = s.date()
        end_d   = (e - dt.timedelta(days=1)).date()
        q = f"repo:{args.owner}/{args.repo} type:issue state:closed closed:{start_d}..{end_d}"
        for it in paged(sess, SEARCH, {"q": q}, f"closed issues {start_d}..{end_d}"):
            num = it.get("number")
            if not num:
                continue
            j,_ = get_json(sess,
                           (ISSUES + "/{num}").format(owner=args.owner, repo=args.repo, num=num),
                           {},
                           "issue detail")
            closed_by = ((j.get("closed_by") or {}).get("login") or "").lower()
            if closed_by:
                issues_closed_by[closed_by] += 1
        if tqdm: bar.update(1)
    if tqdm: bar.close()

    # historical activity
    hist_authors = set()
    hist_start = dt.date(2014,1,1)
    if tqdm: bar = tqdm(total=0, desc="Historical commits", unit="slice")
    for s, e in month_slices(hist_start, SINCE.date(), 6):
        for c in paged(sess,
                       (REPO + "/commits").format(owner=args.owner, repo=args.repo),
                       {"since": z(s), "until": z(e)},
                       f"hist commits {s}..{e}"):
            u = (c.get("author") or {}).get("login")
            if u:
                hist_authors.add(u.lower())
        if tqdm: bar.update(1)
    if tqdm: bar.close()

    hist_pr_authors = set()
    base_hist = f"repo:{args.owner}/{args.repo} type:pr"
    for it in paged(sess, SEARCH, {"q": f"{base_hist} created:2014-01-01..{args.since}"}, "hist PRs"):
        u = (it.get("user") or {}).get("login")
        if u:
            hist_pr_authors.add(u.lower())
    historical_before = hist_authors | hist_pr_authors

    #optional: per-author PR review latency
    pr_review_lat_by_author = defaultdict(list)
    if not args.skip_review_latency:
        if tqdm: bar = tqdm(total=sum(len(v) for v in pr_numbers_by_author.values()),
                            desc="PR review latency", unit="pr")
        for author, nums in pr_numbers_by_author.items():
            for num in nums:
                created_at = None
                prj,_ = get_json(sess,
                                 (PULLS + "/{num}").format(owner=args.owner, repo=args.repo, num=num),
                                 {},
                                 "pr detail")
                created_at = iso(prj["created_at"])
                first_t = None
                for c in paged(sess,
                               (ISSUES + "/{num}/comments").format(owner=args.owner, repo=args.repo, num=num),
                               {},
                               "pr issue comments"):
                    u = (c.get("user") or {}).get("login","").lower()
                    if u in maintainers:
                        t = iso(c["created_at"])
                        if t >= created_at and (first_t is None or t < first_t):
                            first_t = t
                for r in paged(sess,
                               (PULLS + "/{num}/reviews").format(owner=args.owner, repo=args.repo, num=num),
                               {},
                               "pr reviews"):
                    u = (r.get("user") or {}).get("login","").lower()
                    if u in maintainers and r.get("submitted_at"):
                        t = iso(r["submitted_at"])
                        if t >= created_at and (first_t is None or t < first_t):
                            first_t = t
                if first_t:
                    hours = (first_t - created_at).total_seconds() / 3600.0
                    pr_review_lat_by_author[author].append(hours)
                if tqdm: bar.update(1)
        if tqdm: bar.close()

    #bucket classification
    buckets = {
        "Core": [],
        "Frequent": [],
        "Occasional": [],
        "Newcomer": [],
    }

    active_users = set(commits_by.keys()) | set(merged_prs_by.keys()) | set(issues_closed_by.keys())

    for login in active_users:
        c = commits_by.get(login, 0)
        m = merged_prs_by.get(login, 0)
        if login not in historical_before:
            bucket = "Newcomer"
        else:
            if c >= 200 or m >= 150:
                bucket = "Core"
            elif (20 <= c <= 199) or (25 <= m <= 149):
                bucket = "Frequent"
            elif (2 <= c <= 19) or (1 <= m <= 24):
                bucket = "Occasional"
            else:
                bucket = "Occasional"
        buckets[bucket].append(login)

    #aggregate per bucket
    def mean_or_0(vals):
        return sum(vals)/len(vals) if vals else 0.0

    results = []
    for bname, users in buckets.items():
        avg_commits = mean_or_0([commits_by.get(u,0) for u in users])
        avg_issues  = mean_or_0([issues_closed_by.get(u,0) for u in users])
        avg_prs     = mean_or_0([merged_prs_by.get(u,0) for u in users])
        if not args.skip_review_latency:
            all_lat = []
            for u in users:
                all_lat.extend(pr_review_lat_by_author.get(u, []))
            mean_resp_days = (sum(all_lat)/len(all_lat))/24.0 if all_lat else float("nan")
        else:
            mean_resp_days = float("nan")

        results.append((bname, len(users), avg_commits, avg_issues, avg_prs, mean_resp_days))

    #print table
    print("\n=== Contributor Buckets – Activity Profile ===")
    print(f"{'Category':12s} {'Users':>6s} {'Avg.Commits':>13s} {'Avg.Issues':>12s} {'Avg.MergedPRs':>14s} {'Mean PR resp (days)':>20s}")
    for (bname, count, ac, ai, ap, md) in results:
        md_str = f"{md:.1f}" if not math.isnan(md) else "n/a"
        print(f"{bname:12s} {count:6d} {ac:13.1f} {ai:12.1f} {ap:14.1f} {md_str:>20s}")

if __name__ == "__main__":
    main()
