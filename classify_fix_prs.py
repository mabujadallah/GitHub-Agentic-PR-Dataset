#!/usr/bin/env python3
"""
Classify PRs as 'fix' or 'other' based on title pattern matching only.
Reads agent_pull_requests.parquet and human_pull_requests.parquet from the collector output.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data_dec2024_feb2026")
OUTPUT_DIR = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)

FIX_PATTERN = re.compile(
    r"(^fix(\([^)]*\))?!?[\s:!/])"       # conventional: fix:, fix(scope):, fix!:
    r"|(\bfix(es|ed|ing)?\b)"             # natural language: fix, fixes, fixed, fixing
    r"|(\bbug\s*fix\b)"                   # "bugfix" or "bug fix"
    r"|(\bhotfix\b)"                      # hotfix
    r"|(\bbug\b)"                         # bug
    r"|(\bpatch\b)"                       # patch
    r"|(\bresolv(e[ds]?|ing)\b)"          # resolve, resolved, resolves, resolving
    r"|(\berror\b)"                       # error
    r"|(\bcrash(es|ed|ing)?\b)"           # crash, crashes, crashed, crashing
    r"|(\bdefect\b)"                      # defect
    r"|(\bregression\b)"                  # regression
    r"|(\bbroken\b)",                     # broken
    flags=re.IGNORECASE,
)


def classify_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return "other"
    return "fix" if FIX_PATTERN.search(title) else "other"


def process_file(path: Path, source_label: str) -> pd.DataFrame:
    print(f"Loading {path} ...")
    df = pd.read_parquet(path)
    print(f"  {len(df):,} PRs loaded")

    df["type"] = df["title"].apply(classify_title)
    df["source"] = source_label

    fix_count = (df["type"] == "fix").sum()
    other_count = len(df) - fix_count
    print(f"  fix: {fix_count:,}  other: {other_count:,}")
    return df


def main() -> None:
    agent_path = DATA_DIR / "agent_pull_requests.parquet"
    human_path = DATA_DIR / "human_pull_requests.parquet"

    for p in (agent_path, human_path):
        if not p.exists():
            print(f"ERROR: {p} not found. Run the collector first.")
            sys.exit(1)

    agent_df = process_file(agent_path, "agent")
    human_df = process_file(human_path, "human")

    combined = pd.concat([agent_df, human_df], ignore_index=True)

    out_path = OUTPUT_DIR / "fix_classified_prs.parquet"
    combined.to_parquet(out_path, index=False)
    print(f"\nSaved {len(combined):,} classified PRs to {out_path}")

    # Summary
    print("\n=== Summary ===")
    summary = combined.groupby(["source", "type"]).size().unstack(fill_value=0)
    print(summary)

    fix_only = combined[combined["type"] == "fix"]
    fix_path = OUTPUT_DIR / "fix_prs_only.parquet"
    fix_only.to_parquet(fix_path, index=False)
    print(f"\nSaved {len(fix_only):,} fix PRs to {fix_path}")

    # Filter commits and commit details to fix PRs only
    fix_ids = set(fix_only["id"])

    commits_path = DATA_DIR / "pr_commits.parquet"
    if commits_path.exists():
        print("\nFiltering commits to fix PRs ...")
        commits_df = pd.read_parquet(commits_path)
        fix_commits = commits_df[commits_df["pr_id"].isin(fix_ids)]
        fix_commits_path = OUTPUT_DIR / "fix_pr_commits.parquet"
        fix_commits.to_parquet(fix_commits_path, index=False)
        print(f"  {len(fix_commits):,} / {len(commits_df):,} commits -> {fix_commits_path}")

        details_path = DATA_DIR / "pr_commit_details.parquet"
        if details_path.exists():
            print("Filtering commit details to fix PRs (lean read) ...")
            # Read only needed columns to avoid loading 39 GB patch column
            details_df = pd.read_parquet(details_path, columns=[
                "sha", "pr_id", "filename", "status",
                "additions", "deletions", "changes",
                "commit_stats_total", "commit_stats_additions", "commit_stats_deletions",
            ])
            fix_details = details_df[details_df["pr_id"].isin(fix_ids)]
            fix_details_path = OUTPUT_DIR / "fix_pr_commit_details.parquet"
            fix_details.to_parquet(fix_details_path, index=False)
            print(f"  {len(fix_details):,} / {len(details_df):,} detail rows -> {fix_details_path}")
    else:
        print(f"\n{commits_path} not found — skipping commit/detail filtering.")


if __name__ == "__main__":
    main()
