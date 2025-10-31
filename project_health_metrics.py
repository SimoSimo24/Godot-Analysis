
import argparse, datetime as dt, time, random, math, statistics as stats, json, os, sys
from collections import defaultdict, Counter
from typing import Dict, Iterable, List, Optional, Set, Tuple
import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

SEARCH = "https://api.github.com/search/issues"
REPO = "https://api.github.com/repos/{owner}/{repo}"
PULLS = REPO + "/pulls"
ISSUES = REPO + "/issues"
ORG_MEMBERS = "https://api.github.com/orgs/{org}/members"


def z(dtobj: dt.datetime) -> str:
    return dtobj.isoformat() + "Z"

def iso(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z","+00:00")).replace(tzinfo=None)

def dtrange_months(start: dt.date, end: dt.date, step_months=1):
    cur = dt.date(start.year, start.month, 1)
    end1 = dt.date(end.year, end.month, 1)
    while cur < end1:
        m = cur.month - 1 + step_months
        y = cur.year + m // 12
        m = m % 12 + 1
        nxt = dt.date(y, m, 1)
        yield dt.datetime.combine(cur, dt.time.min), dt.datetime.combine(min(nxt, end1), dt.time.min)
        cur = nxt

def rate_sleep(resp):
    if resp.status_code==403 and "rate limit" in resp.text.lower():
        reset = int(resp.headers.get("X-RateLimit-Reset","0"))
        now   = int(time.time())
        wait  = max(5, reset-now+1)
        (tqdm.write if tqdm else print)(f"[rate limit] sleeping {wait}s")
        time.sleep(wait)
        return True
    return False

def get_json(sess, url, params=None, desc=""):
    while True:
        r = sess.get(url, params=params)
        if rate_sleep(r): continue
        if r.status_code >= 500:
            back = min(30, 2**random.randint(0,5)) + random.uniform(0,1.2)
            (tqdm.write if tqdm else print)(f"[{desc}] {r.status_code}, retry in {back:.1f}s")
            time.sleep(back); continue
        r.raise_for_status()
        return r.json(), r.links

def paged(sess, url, params, desc=""):
    page=1
    while True:
        params = dict(params or {}, per_page=100, page=page)
        j, links = get_json(sess, url, params, desc)
        if isinstance(j, dict):  
            items = j.get("items", [])
            for it in items: yield it
            if len(items)<100 or page>=10: break   # Search API hard cap
        else:
            if not j: break
            for it in j: yield it
            if 'next' not in links: break
        page+=1

def cp_path(name: str) -> str:
    return f".ph_checkpoint_{name}.json"

def cp_load(name: str, default):
    path = cp_path(name)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def cp_save(name: str, data):
    path = cp_path(name)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)

def cp_cleanup_all():
    for fn in os.listdir("."):
        if fn.startswith(".ph_checkpoint_") and fn.endswith(".json"):
            try:
                os.remove(fn)
            except Exception:
                pass


def load_org_members(sess, owner: str) -> Set[str]:
    members=set()
    try:
        for m in paged(sess, ORG_MEMBERS.format(org=owner), {}, "org_members"):
            lg = (m.get("login") or "").lower()
            if lg: members.add(lg)
    except requests.HTTPError:
        pass
    return members


def main():
    ap = argparse.ArgumentParser(description="Project Health Metrics (fast) from GitHub API")
    ap.add_argument("--owner", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--since", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--until", required=True, help="YYYY-MM-DD exclusive")
    ap.add_argument("--token", required=True)
    ap.add_argument("--slice-months", type=int, default=1)
    ap.add_argument("--skip-review-latency", action="store_true",
                    help="Skip PR review-time metric for faster runs")
    ap.add_argument("--review-sample", type=int, default=0,
                    help="Sample N PRs for review latency instead of scanning all (0=all)")
    args = ap.parse_args()

    SINCE = dt.datetime.fromisoformat(args.since)
    UNTIL = dt.datetime.fromisoformat(args.until)

    sess = requests.Session()
    sess.headers.update({"Accept":"application/vnd.github+json","Authorization":f"Bearer {args.token}"})

    maintainers = load_org_members(sess, args.owner)

    prs = cp_load("prs", [])
    if prs:
        (tqdm.write if tqdm else print)(f"[CP] loaded PRs: {len(prs)}")
    else:
        if tqdm: bar = tqdm(total=0, desc="PRs (created per-month)", unit="item")
        for s, e in dtrange_months(SINCE.date(), UNTIL.date(), args.slice_months):
            start_d = s.date()
            end_d   = (e - dt.timedelta(days=1)).date()
            q = f'repo:{args.owner}/{args.repo} type:pr created:{start_d}..{end_d}'
            month_count = 0
            for it in paged(sess, SEARCH, {"q": q}, f"prs {start_d}..{end_d}"):
                if "pull_request" not in it:
                    continue
                prs.append(it)
                month_count += 1
            cp_save("prs", prs)
            if tqdm:
                tqdm.write(f"[PRs] {start_d}..{end_d}: +{month_count} (total={len(prs)})")
        if tqdm: bar.close()

    merged_prs = cp_load("merged_prs", [])
    if merged_prs:
        (tqdm.write if tqdm else print)(f"[CP] loaded merged PRs: {len(merged_prs)}")
    else:
        if tqdm: bar = tqdm(total=0, desc="Merged PRs (per-month)", unit="item")
        for s, e in dtrange_months(SINCE.date(), UNTIL.date(), args.slice_months):
            start_d = s.date()
            end_d   = (e - dt.timedelta(days=1)).date()
            q = f'repo:{args.owner}/{args.repo} type:pr is:merged created:{start_d}..{end_d}'
            month_count = 0
            for it in paged(sess, SEARCH, {"q": q}, f"merged PRs {start_d}..{end_d}"):
                if "pull_request" not in it:
                    continue
                merged_prs.append(it)
                month_count += 1
            cp_save("merged_prs", merged_prs)
            if tqdm:
                tqdm.write(f"[Merged PRs] {start_d}..{end_d}: +{month_count} (total={len(merged_prs)})")
        if tqdm: bar.close()

    mergers=set()
    mergers = set(cp_load("mergers", []))
    if mergers:
        (tqdm.write if tqdm else print)(f"[CP] loaded mergers: {len(mergers)}")
    else:
        if tqdm: bar = tqdm(total=len(merged_prs), desc="Reading merged_by", unit="pr")
        for it in merged_prs:
            num = it.get("number")
            if not num:
                if tqdm: bar.update(1)
                continue
            j,_ = get_json(sess, (PULLS+"/{num}").format(owner=args.owner, repo=args.repo, num=num), {}, "pull(merged_by)")
            if j.get("merged_at"):
                mb = (j.get("merged_by") or {}).get("login")
                if mb: mergers.add(mb.lower())
            if tqdm: bar.update(1)
        cp_save("mergers", sorted(list(mergers)))
        if tqdm: bar.close()
    maintainers |= {m.lower() for m in mergers}

    pr_first_review_hours=[]
    if not args.skip_review_latency:
        scan_prs = prs
        if args.review_sample and args.review_sample < len(prs):
            import random as _random
            _random.seed(42)
            scan_prs = _random.sample(prs, args.review_sample)
            (tqdm.write if tqdm else print)(f"[sample] review latency on {len(scan_prs)}/{len(prs)} PRs")

        done_idx = int(cp_load("review_done_idx", 0) or 0)
        pr_first_review_hours = cp_load("review_hours", [])
        if pr_first_review_hours and done_idx:
            (tqdm.write if tqdm else print)(f"[CP] resume PR review at index {done_idx}")

        if tqdm: bar = tqdm(total=len(scan_prs), desc="PR review time", unit="pr", initial=done_idx)
        for idx in range(done_idx, len(scan_prs)):
            it = scan_prs[idx]
            num = it.get("number")
            created = iso(it["created_at"])
            first_t = None

            for c in paged(sess, (ISSUES+"/{num}/comments").format(owner=args.owner, repo=args.repo, num=num), {}, "pr_issue_comments"):
                u=(c.get("user") or {}).get("login","").lower()
                if u in maintainers:
                    t = iso(c["created_at"])
                    if t>=created and (first_t is None or t<first_t):
                        first_t=t

            for r in paged(sess, (PULLS+"/{num}/reviews").format(owner=args.owner, repo=args.repo, num=num), {}, "pr_reviews"):
                u=(r.get("user") or {}).get("login","").lower()
                if u in maintainers and r.get("submitted_at"):
                    t=iso(r["submitted_at"])
                    if t>=created and (first_t is None or t<first_t):
                        first_t=t

            if first_t:
                pr_first_review_hours.append((first_t-created).total_seconds()/3600.0)

            if (idx+1) % 50 == 0:
                cp_save("review_done_idx", idx+1)
                cp_save("review_hours", pr_first_review_hours)

            if tqdm: bar.update(1)
        if tqdm: bar.close()
        cp_save("review_done_idx", len(scan_prs))
        cp_save("review_hours", pr_first_review_hours)
    else:
        (tqdm.write if tqdm else print)("Skipping PR review latency (--skip-review-latency)")

    pr_merge_hours = cp_load("pr_merge_hours", [])
    if pr_merge_hours:
        (tqdm.write if tqdm else print)(f"[CP] loaded merge hours ({len(pr_merge_hours)})")
    else:
        if tqdm: bar = tqdm(total=len(merged_prs), desc="PR merge time", unit="pr")
        for it in merged_prs:
            num = it.get("number")
            j,_ = get_json(sess, (PULLS+"/{num}").format(owner=args.owner, repo=args.repo, num=num), {}, "pull_merge_time")
            if j.get("merged_at"):
                pr_merge_hours.append((iso(j["merged_at"]) - iso(j["created_at"])).total_seconds()/3600.0)
            if tqdm: bar.update(1)
        if tqdm: bar.close()
        cp_save("pr_merge_hours", pr_merge_hours)

    issues = cp_load("issues", [])
    if issues:
        (tqdm.write if tqdm else print)(f"[CP] loaded issues: {len(issues)}")
    else:
        if tqdm: bar = tqdm(total=0, desc="Issues (created per-month)", unit="item")
        for s, e in dtrange_months(SINCE.date(), UNTIL.date(), args.slice_months):
            start_d = s.date()
            end_d   = (e - dt.timedelta(days=1)).date()
            q = f'repo:{args.owner}/{args.repo} type:issue created:{start_d}..{end_d}'
            month_count = 0
            for it in paged(sess, SEARCH, {"q": q}, f"issues {start_d}..{end_d}"):
                if "pull_request" in it:
                    continue
                issues.append(it)
                month_count += 1
            cp_save("issues", issues)
            if tqdm:
                tqdm.write(f"[Issues] {start_d}..{end_d}: +{month_count} (total={len(issues)})")
        if tqdm: bar.close()

    issue_resp_hours = cp_load("issue_resp_hours", [])
    if issue_resp_hours:
        (tqdm.write if tqdm else print)(f"[CP] loaded issue response samples ({len(issue_resp_hours)})")
    else:
        if tqdm: bar = tqdm(total=len(issues), desc="Issue metrics", unit="issue")
        for it in issues:
            num = it.get("number")
            created = iso(it["created_at"])
            first = None
            for c in paged(sess, (ISSUES+"/{num}/comments").format(owner=args.owner, repo=args.repo, num=num), {}, "issue_comments"):
                t = iso(c["created_at"])
                if t>=created:
                    first = t; break
            if first:
                issue_resp_hours.append((first-created).total_seconds()/3600.0)
            if tqdm: bar.update(1)
        if tqdm: bar.close()
        cp_save("issue_resp_hours", issue_resp_hours)
    closed_count = sum(1 for it in issues if it.get("state")=="closed")
    total_issues=len(issues)
    closure_rate = (closed_count/total_issues) if total_issues>0 else float('nan')

    active_months_by=defaultdict(set)
    commits_loaded = cp_load("commits_active_months", None)
    if commits_loaded is not None:
        active_months_by = defaultdict(set, {k:set(map(tuple,v)) for k,v in commits_loaded.items()})
        (tqdm.write if tqdm else print)(f"[CP] loaded commit-active months for {len(active_months_by)} users")
    else:
        if tqdm: bar = tqdm(total=0, desc="Commits", unit="slice")
        for s,e in dtrange_months(SINCE.date(), UNTIL.date(), args.slice_months):
            for c in paged(sess, (REPO+"/commits").format(owner=args.owner, repo=args.repo),
                           {"since": z(s), "until": z(e)}, "commits"):
                u=(c.get("author") or {}).get("login")
                if u:
                    active_months_by[u.lower()].add((s.year,s.month))
            cp_save("commits_active_months", {k:list(v) for k,v in active_months_by.items()})
        if tqdm: bar.close()

    for it in prs:
        u=(it.get("user") or {}).get("login")
        if u:
            t=iso(it["created_at"])
            active_months_by[u.lower()].add((t.year,t.month))

    active_contributors=len(active_months_by)
    retained=sum(1 for mset in active_months_by.values() if len(mset)>=3)
    retention_rate = (retained/active_contributors) if active_contributors>0 else float('nan')

    bus_factor=len(mergers)

    first_pr_merged=[]
    prs_by_author=defaultdict(list)
    for it in prs:
        u=(it.get("user") or {}).get("login")
        if u: prs_by_author[u.lower()].append(it)
    for u, plist in prs_by_author.items():
        plist.sort(key=lambda x: x["created_at"])
        first=plist[0]
        first_created=iso(first["created_at"])
        if SINCE <= first_created < UNTIL:
            num=first.get("number")
            j,_ = get_json(sess, (PULLS+"/{num}").format(owner=args.owner, repo=args.repo, num=num), {}, "first_pr_check")
            first_pr_merged.append(bool(j.get("merged_at")))
    newcomer_merge_success = (sum(1 for x in first_pr_merged if x)/len(first_pr_merged)) if first_pr_merged else float('nan')

    def med_hours_to_days(vals):
        return round(stats.median(vals)/24.0, 1) if vals else float('nan')

    median_pr_review_days = med_hours_to_days(pr_first_review_hours)
    median_pr_merge_days  = med_hours_to_days(pr_merge_hours)
    issue_resp_days       = med_hours_to_days(issue_resp_hours)

    print("\n=== Project Health (12-month window) ===")
    print(f"Median PR Review Time (to first maintainer comment): {median_pr_review_days} days")
    print(f"Median PR Merge Time:                                {median_pr_merge_days} days")
    print(f"Issue Response Latency (to first comment):           {issue_resp_days} days")
    print(f"Issue Closure Rate:                                  {round(closure_rate*100,1) if not math.isnan(closure_rate) else 'NA'}%")
    print(f"Contributor Retention Rate (≥3 months active):       {round(retention_rate*100,1) if not math.isnan(retention_rate) else 'NA'}%")
    print(f"Bus Factor (mergers):                                {bus_factor}")
    print(f"Newcomer Merge Success Rate:                         {round(newcomer_merge_success*100,1) if not math.isnan(newcomer_merge_success) else 'NA'}%")

    cp_cleanup_all()

if __name__ == "__main__":
    main()
