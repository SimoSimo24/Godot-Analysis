🧠 Godot Repository Analysis Toolkit

This repository contains a collection of Python scripts designed to analyze the health, contributor activity, and project quality of the Godot Engine repository. The scripts extract metrics on contributions, classify users, detect bots and duplicate accounts, and compute activity and health indicators using the GitHub REST API.

⚙️ Requirements
Before running any script, install the following dependencies:

pip install requests tqdm

All scripts require a GitHub Personal Access Token (PAT) with public_repo access to avoid rate limits.

You can export it as an environment variable for convenience:
export GITHUB_TOKEN="your_token_here"

🧩 Scripts Overview & Commands

1. project_health_metrics.py
Purpose: Calculates overall project health indicators such as median PR review time, merge time, issue response time, issue closure rate, retention rate, and bus factor.

Example usage:
python project_health_metrics.py --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01 --token $GITHUB_TOKEN --slice-months 1

Optional flags:
--skip-review-latency → skip slow review latency computation
--review-sample N → only sample N PRs for latency computation

2. bucket_activity.py
Purpose: Classifies contributors into activity buckets (Core, Frequent, Occasional, Newcomer) based on their commits, merged PRs, and closed issues.

Example usage:
python bucket_activity.py --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01 --token $GITHUB_TOKEN --slice-months 1

Optional:
--skip-review-latency to skip expensive PR latency collection.

3. contributor_buckets.py
Purpose: Computes contributor buckets (Key, Frequent, Occasional, Newcomer, Dormant) with adaptive search to bypass GitHub’s 1000-item limit.

Example usage:
python contributor_buckets.py --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01 --token $GITHUB_TOKEN --slice-months 1 --historical-start 2014-01-01

4. bot_filter_username.py
Purpose: Detects likely bot accounts using username patterns (e.g., *-bot, [bot], or known CI services).

Example usage:
python bot_filter_username.py --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01 --token $GITHUB_TOKEN --slice-months 1 --include-reviews

Optional flags:
--skip-issue-comments
--skip-review-comments
--include-reviews to analyze PR reviewers.

5. bot_filter_pattern.py
Purpose: Detects bots without relying on username patterns — classifies accounts based on activity patterns, API usage, and profile metadata.

Example usage:
python bot_filter_pattern.py --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01 --token $GITHUB_TOKEN --slice-months 1 --include-issue-comments --include-review-comments

6. dedupe_email.py
Purpose: Identifies duplicate contributor accounts by matching shared commit emails (e.g., GitHub “noreply” addresses).

Example usage:
python dedupe_email.py --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01 --token $GITHUB_TOKEN

7. dedupe_profile.py
Purpose: Detects suspicious duplicate accounts based on GitHub profile similarity (name, blog/company, creation date).

Example usage:
python dedupe_profile.py --token $GITHUB_TOKEN --owner godotengine --repo godot --since 2024-10-01 --until 2025-10-01

Or using a predefined CSV of logins:
python dedupe_profile.py --token $GITHUB_TOKEN --input-logins contributors_godotengine_godot_2024-10-01_2025-10-01.csv

🗃️ Output Files
Each script generates a CSV file in the working directory, for example:
- buckets_godotengine_godot_2024-10-01_2025-10-01.csv
- bots_username_godotengine_godot_2024-10-01_2025-10-01.csv
- dedupe_by_email_godotengine_godot_2024-10-01_2025-10-01.csv
- bots_nousername_godotengine_godot_2024-10-01_2025-10-01.csv

📊 Recommended Workflow
1. Detect and filter bots:
   python bot_filter_username.py ...
   python bot_filter_pattern.py ...
2. Identify duplicate accounts:
   python dedupe_email.py ...
   python dedupe_profile.py ...
3. Classify contributors by activity:
   python contributor_buckets.py ...
4. Compute project health metrics:
   python project_health_metrics.py ...
5. Summarize and visualize in your report.

🧾 Notes
- Long-running scripts such as project_health_metrics.py can take several hours due to API pagination.
- Progress bars will appear automatically if tqdm is installed.
- Use smaller time windows (--slice-months 1) to resume from partial results.
