import pandas as pd
import requests
import time
import os
import json
import threading
import subprocess
import shutil
import re as _re
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKENS = os.getenv("GITHUB_TOKENS", "").split(",")
GITHUB_TOKENS = [t.strip() for t in GITHUB_TOKENS if t.strip()]

if not GITHUB_TOKENS:
    raise RuntimeError(
        "GITHUB_TOKENS is empty — set comma-separated tokens in .env "
        "(needed for PR and commit collection phases)"
    )

# 8 workers per token — rate limit is hourly so we have plenty of headroom
NUM_WORKERS = len(GITHUB_TOKENS) * 8

print(f"Loaded {len(GITHUB_TOKENS)} tokens from .env")
print(f"Parallel workers: {NUM_WORKERS}")
_startup_proxy = os.getenv("HTTPS_PROXY", "").strip()
print(f"Proxy: {_startup_proxy if _startup_proxy else 'disabled'}")

# Telegram monitoring (set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID in .env)
_TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

# Hot .env reload tracking
try:
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
except NameError:
    _env_path = os.path.abspath(".env")
_env_mtime   = os.path.getmtime(_env_path) if os.path.exists(_env_path) else 0.0
_tokens_lock = threading.Lock()

# Thread-local sessions — each thread reuses its own TCP/TLS connections (much faster)
_thread_local = threading.local()

def get_session(token_idx):
    """Get or create a requests.Session for this thread with connection pooling."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        _thread_local.session.headers["Accept"] = "application/vnd.github.v3+json"
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        _thread_local.session.mount("https://", adapter)
    token = GITHUB_TOKENS[token_idx % len(GITHUB_TOKENS)]
    _thread_local.session.headers["Authorization"] = f"token {token}"
    # Re-apply proxy on every call so .env hot-reload takes effect immediately.
    # Set HTTPS_PROXY=http://user:pass@host:port in .env to enable; leave blank to disable.
    _proxy = os.getenv("HTTPS_PROXY", "").strip()
    _thread_local.session.proxies = {"https": _proxy, "http": _proxy} if _proxy else {}
    return _thread_local.session

# Test all tokens
for i, token in enumerate(GITHUB_TOKENS):
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    try:
        r = requests.get("https://api.github.com/rate_limit", headers=headers, timeout=15)
        if r.status_code == 200:
            remaining = r.json()['rate']['remaining']
            limit = r.json()['rate']['limit']
            print(f"  Token {i+1}: OK — {remaining}/{limit} remaining")
        else:
            print(f"  Token {i+1}: FAILED — status {r.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"  Token {i+1}: NETWORK ERROR — {e}")

def github_get(url, params=None, token_idx=None):
    """
    GET from GitHub API using persistent sessions (connection reuse).
    token_idx selects which token to authenticate with.
    """
    idx = token_idx if token_idx is not None else 0
    for attempt in range(len(GITHUB_TOKENS) * 2):
        session = get_session(idx)
        try:
            r = session.get(url, params=params, timeout=30)
        except requests.exceptions.Timeout:
            print(f"  Timeout: {url}", flush=True)
            idx = (idx + 1) % len(GITHUB_TOKENS)
            continue
        except requests.exceptions.RequestException as e:
            msg = str(e).lower()
            transient = (
                'nameresolution' in msg or 'name resolution' in msg or 'failed to resolve' in msg
                or 'connection aborted' in msg or 'remotedisconnected' in msg
                or 'remote end closed' in msg or 'connection reset' in msg
                or 'ssleoferror' in msg or 'unexpected_eof' in msg or 'eof occurred' in msg
            )
            if transient:
                wait = min(30, 2 ** attempt)
                print(f"  Connection error (attempt {attempt}) — retrying in {wait}s: {e}", flush=True)
                time.sleep(wait)
                continue
            print(f"  Request error: {e}", flush=True)
            return None

        if r.status_code == 200:
            return r.json()

        if r.status_code == 403 and 'rate limit' in r.text.lower():
            remaining = int(r.headers.get('X-RateLimit-Remaining', 0))
            if remaining == 0:
                # Rotate to the next token immediately — don't block this thread.
                # Only sleep if we've exhausted all tokens (full lap).
                next_idx = (idx + 1) % len(GITHUB_TOKENS)
                if next_idx == (token_idx or 0) % len(GITHUB_TOKENS):
                    reset_time = int(r.headers.get('X-RateLimit-Reset', 0))
                    wait = max(reset_time - int(time.time()), 10)
                    print(f"  All tokens rate limited. Waiting {wait}s...", flush=True)
                    time.sleep(wait)
                else:
                    pass
                    # print(f"  Token {idx} rate limited — rotating to token {next_idx}.", flush=True)
                idx = next_idx
            else:
                idx = (idx + 1) % len(GITHUB_TOKENS)
            continue

        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 60))
            print(f"  Secondary rate limit (429) — sleeping {retry_after}s...", flush=True)
            time.sleep(retry_after)
            continue  # same token — secondary limits are per-IP not per-token

        if r.status_code == 404:
            print(f"  Not Found (404): {url}", flush=True)
            return None

        # Retry on server errors (500, 502, 503) with exponential backoff
        if r.status_code >= 500:
            wait = min(2 ** attempt, 30)
            print(f"  Server {r.status_code} on {url} — retrying in {wait}s...", flush=True)
            time.sleep(wait)
            idx = (idx + 1) % len(GITHUB_TOKENS)
            continue

        print(f"  Error {r.status_code}: {url}", flush=True)
        return None

    print(f"  Max retries exceeded: {url}", flush=True)
    return None


def github_get_all_pages(url, params=None, token_idx=None):
    if params is None:
        params = {}
    params = {**params, 'per_page': 100, 'page': 1}

    all_items = []
    while True:
        data = github_get(url, params, token_idx=token_idx)
        if data is None:
            return None # Propagate error
        if len(data) == 0:
            break
        all_items.extend(data)
        if len(data) < 100:
            break
        params = {**params, 'page': params['page'] + 1}

    return all_items

print("Helpers ready (session-pooled, thread-safe).")


def send_telegram(msg: str):
    """Send a message via Telegram Bot API. Silent no-op if env vars not set."""
    if not _TG_TOKEN or not _TG_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            json={"chat_id": _TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass  # Never let Telegram failures interrupt collection


# Shared progress state — updated by collection phases, read by heartbeat thread
_progress = {
    "phase": "startup",
    "repos_done": 0, "repos_total": 0,
    "prs_done": 0,   "prs_total": 0,
    "commits_done": 0,
}


def _query_token_status() -> str:
    """Query rate_limit for all tokens; return a formatted status string."""
    lines = []
    for i, token in enumerate(GITHUB_TOKENS):
        try:
            r = requests.get(
                "https://api.github.com/rate_limit",
                headers={"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"},
                timeout=10,
            )
            if r.status_code == 200:
                d = r.json()["rate"]
                lines.append(f"  Token {i+1}: {d['remaining']}/{d['limit']}")
            else:
                lines.append(f"  Token {i+1}: HTTP {r.status_code}")
        except Exception as e:
            lines.append(f"  Token {i+1}: error ({e})")
    return "\n".join(lines)


def _heartbeat():
    """Background thread: send a Telegram status update every 5 minutes."""
    while True:
        time.sleep(300)
        try:
            p = _progress
            token_status = _query_token_status()
            msg = (
                f"<b>Collector heartbeat</b>\n"
                f"Phase: {p['phase']}\n"
                f"Repos: {p['repos_done']}/{p['repos_total']}\n"
                f"PRs: {p['prs_done']:,}/{p['prs_total']:,}\n"
                f"Commits: {p['commits_done']:,}\n\n"
                f"<b>Tokens:</b>\n{token_status}"
            )
            send_telegram(msg)
        except Exception:
            pass


def _load_tokens():
    return [t.strip() for t in os.getenv("GITHUB_TOKENS", "").split(",") if t.strip()]


def _env_watcher():
    """Background thread: hot-reload GITHUB_TOKENS when .env changes on disk."""
    global _env_mtime
    while True:
        time.sleep(30)
        try:
            if not os.path.exists(_env_path):
                continue
            mtime = os.path.getmtime(_env_path)
            if mtime != _env_mtime:
                load_dotenv(_env_path, override=True)
                new_tokens = _load_tokens()
                with _tokens_lock:
                    GITHUB_TOKENS[:] = new_tokens
                    _env_mtime = mtime
                print(f"  [env-watcher] Reloaded .env: {len(new_tokens)} tokens", flush=True)
                send_telegram(f"[env-watcher] Reloaded .env — {len(new_tokens)} tokens active.")
        except Exception as e:
            print(f"  [env-watcher] Error: {e}", flush=True)


# Start background threads (daemon=True so they don't block process exit)
_t_heartbeat   = threading.Thread(target=_heartbeat,   daemon=True, name="heartbeat")
_t_env_watcher = threading.Thread(target=_env_watcher, daemon=True, name="env-watcher")
_t_heartbeat.start()
_t_env_watcher.start()

send_telegram(
    f"<b>Collector started</b>\n"
    f"Tokens: {len(GITHUB_TOKENS)}\n"
    f"Workers: {NUM_WORKERS}\n"
    f"Proxy: {_startup_proxy if _startup_proxy else 'disabled'}\n\n"
    f"<b>Token status:</b>\n{_query_token_status()}"
)

print("Loading AIDev-pop repository table...")
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
print(f"Total AIDev-pop repos: {len(repo_df):,}")

AGENTS_TO_KEEP = ['Devin', 'Copilot', 'Cursor', 'Claude_Code']

# Load only the 3 columns we need, and push the agent filter down to the
# Parquet reader so the full table is never materialized in RAM.
print("Loading all_pull_request table (memory-efficient, filtered)...")
aidev_filtered = pd.read_parquet(
    "hf://datasets/hao-li/AIDev/all_pull_request.parquet",
    columns=['repo_id', 'user', 'agent'],
    filters=[('agent', 'in', AGENTS_TO_KEEP)],
)
print(f"PRs after filtering to 4 agents: {len(aidev_filtered):,}")

agent_repo_ids = set(aidev_filtered['repo_id'].unique())
print(f"Repos with agent PRs: {len(agent_repo_ids)}")

target_repos = repo_df[repo_df['id'].isin(agent_repo_ids)].copy()
# Build repo_id lookup: full_name -> id
repo_id_map = dict(zip(target_repos['full_name'], target_repos['id']))
repos = target_repos['full_name'].tolist()

print(f"Total repos to collect from: {len(repos)}")
print(f"First 10: {repos[:10]}")

agent_user_map = {}
for _, row in aidev_filtered[['user', 'agent']].drop_duplicates().iterrows():
    agent_user_map[row['user']] = row['agent']

print(f"Total known agent usernames: {len(agent_user_map)}")

def classify_pr_author(username):
    if username in agent_user_map:
        return True, agent_user_map[username]
    return False, 'human'

print("Classifier ready.")

# ════════════════════════════════════════════
# 15-MONTH DATE RANGE: Dec 2024 – Feb 2026
# ════════════════════════════════════════════
DATE_FROM = "2024-12-01"
DATE_TO   = "2026-02-28"

CHECKPOINT_DIR = "data_dec2024_feb2026"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ── Local-git commit details configuration ──────────────────────
REPO_CACHE_DIR   = "repo_cache"                                  # where mirror clones live
DETAILS_CHUNK_DIR = f"{CHECKPOINT_DIR}/detail_chunks"             # per-repo parquet chunks
DETAILS_CKPT_FILE = f"{CHECKPOINT_DIR}/checkpoint_details_local.json"
CLONE_WORKERS    = 10         # parallel repo clone/extract/delete threads
INCLUDE_PATCH    = True       # set False to skip patch extraction (much faster)
MAX_PATCH_BYTES  = 1_000_000  # 1 MB — patches larger than this are stored as None
CLONE_TIMEOUT    = 3600       # seconds for git clone (increased for large repos)
FETCH_TIMEOUT    = 600        # seconds for git fetch
EXTRACT_TIMEOUT  = 3600       # seconds for git log/diff-tree extraction per batch
SHA_BATCH_SIZE   = 500        # SHAs per batch for git log/diff-tree
DELETE_AFTER_EXTRACT = True   # delete mirror clone after extracting details (saves disk)
# ────────────────────────────────────────────────────────────────

# Clear old checkpoint files if you want a clean re-run
# (uncomment the next 2 lines to reset)
# import glob
# [os.remove(f) for f in glob.glob(f"{CHECKPOINT_DIR}/checkpoint_*.json")]

print(f"Collecting PRs from {DATE_FROM} to {DATE_TO}")
print(f"Target repos: {len(repos)}")
print(f"Output dir: {CHECKPOINT_DIR}/")

# Preflight DNS check — wait up to ~5 min before giving up
import socket as _socket
for _dns_attempt in range(10):
    try:
        _socket.getaddrinfo("api.github.com", 443)
        print("DNS OK — api.github.com resolves.", flush=True)
        break
    except _socket.gaierror as _e:
        _wait = min(60, 2 ** _dns_attempt)
        print(f"DNS not ready (attempt {_dns_attempt+1}/10): {_e} — retrying in {_wait}s", flush=True)
        time.sleep(_wait)
else:
    raise RuntimeError("api.github.com DNS resolution failed after 10 attempts — check /etc/resolv.conf")

CHECKPOINT_FILE = f"{CHECKPOINT_DIR}/checkpoint_prs.json"

def fetch_repo_prs(repo, token_idx, date_from=None, date_to=None):
    """Fetch PRs for a repo using state=all (single pass)."""
    if date_from is None:
        date_from = DATE_FROM
    if date_to is None:
        date_to = DATE_TO
    repo_prs = []
    url = f"https://api.github.com/repos/{repo}/pulls"
    page = 1

    while True:
        params = {
            'state': 'all', 'sort': 'created', 'direction': 'desc',
            'per_page': 100, 'page': page
        }

        data = github_get(url, params, token_idx=token_idx)
        if data is None:
            return repo, repo_prs, [repo]
        if len(data) == 0:
            break

        stop = False
        for pr in data:
            created = pr.get('created_at', '')
            if created < date_from:
                stop = True
                break
            if created > date_to + "T23:59:59Z":
                continue

            username = pr.get('user', {}).get('login', '')
            is_agent, agent_name = classify_pr_author(username)

            repo_prs.append({
                'id': pr['id'],
                'number': pr['number'],
                'title': pr.get('title', ''),
                'body': pr.get('body', ''),
                'user': username,
                'user_id': pr.get('user', {}).get('id'),
                'state': pr.get('state', ''),
                'created_at': created,
                'closed_at': pr.get('closed_at'),
                'merged_at': pr.get('pull_request', {}).get('merged_at') if pr.get('pull_request') else pr.get('merged_at'),
                'repo_id': repo_id_map.get(repo),
                'repo_url': f"https://api.github.com/repos/{repo}",
                'repo_name': repo,
                'html_url': pr.get('html_url', ''),
                'is_agent': is_agent,
                'agent': agent_name
            })

        if stop or len(data) < 100:
            break
        page += 1

    return repo, repo_prs, []


def _run_pr_collection(repos_list, date_from, date_to, checkpoint_file, label=""):
    """
    Collect PRs for repos_list over the given date range.
    Resumes from checkpoint_file; saves checkpoint atomically every 100 repos.
    Returns (all_prs, done_repos_list, errors).
    """
    done_repos = set()
    all_prs = []
    errors = []
    skipped = []

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                ckpt = json.load(f)
            all_prs = ckpt['all_prs']
            errors = ckpt.get('errors', [])
            skipped = ckpt.get('skipped', [])
            done_repos = set(ckpt.get('done_repos', []))
            print(f"[{label}] RESUMED: {len(done_repos)}/{len(repos_list)} repos done, {len(all_prs)} PRs", flush=True)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[{label}] WARNING: Checkpoint corrupted ({e}), starting fresh.", flush=True)
    else:
        print(f"[{label}] Starting fresh", flush=True)

    remaining = [r for r in repos_list if r not in done_repos]
    print(f"[{label}] Repos remaining: {len(remaining)} (of {len(repos_list)} total)", flush=True)
    print(f"[{label}] Using {NUM_WORKERS} parallel workers (state=all, single pass)", flush=True)

    _progress["phase"] = label or "pr-collection"
    _progress["repos_total"] = len(repos_list)
    _progress["repos_done"] = len(done_repos)
    _progress["prs_done"] = len(all_prs)

    t_start = time.time()
    completed = 0

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}
        for idx, repo in enumerate(remaining):
            token_idx = idx % len(GITHUB_TOKENS)
            fut = executor.submit(fetch_repo_prs, repo, token_idx, date_from, date_to)
            futures[fut] = repo

        for fut in as_completed(futures):
            completed += 1
            try:
                repo, repo_prs, repo_errors = fut.result()
                all_prs.extend(repo_prs)
                errors.extend(repo_errors)
                done_repos.add(repo)
            except Exception as e:
                repo = futures[fut]
                print(f"  EXCEPTION on {repo}: {e}", flush=True)
                skipped.append(repo)
                done_repos.add(repo)

            _progress["repos_done"] = len(done_repos)
            _progress["prs_done"] = len(all_prs)

            if completed % 100 == 0 or completed == len(remaining):
                elapsed = time.time() - t_start
                rate = completed / max(elapsed, 1) * 3600
                print(f"  [{label}] Progress: {len(done_repos)}/{len(repos_list)} repos | {len(all_prs)} PRs | {rate:.0f} repos/hr", flush=True)

                tmp = checkpoint_file + '.tmp'
                with open(tmp, 'w') as f:
                    json.dump({
                        'done_repos': list(done_repos),
                        'all_prs': all_prs,
                        'errors': errors,
                        'skipped': skipped
                    }, f)
                os.replace(tmp, checkpoint_file)

    elapsed_total = time.time() - t_start
    print(f"\n[{label}] Done! Collected {len(all_prs)} PRs from {len(repos_list)} repos in {elapsed_total/60:.1f} min", flush=True)
    print(f"[{label}] Errors: {len(errors)}, Skipped: {len(skipped)}", flush=True)
    return all_prs, list(done_repos), errors


# ════════════════════════════════════════════════════════
# PHASE 0: Jan 2025 Validation Sprint
# Quick one-month collection to validate against AIDev baseline.
# ════════════════════════════════════════════════════════
JAN_CKPT = f"{CHECKPOINT_DIR}/checkpoint_jan2025_validation.json"

print("\n" + "="*60, flush=True)
print("PHASE 0: Jan 2025 validation sprint (2025-01-01 to 2025-01-31)", flush=True)
print("="*60, flush=True)

jan_prs, _, _ = _run_pr_collection(
    repos, "2025-01-01", "2025-01-31", JAN_CKPT, "Jan2025")

if jan_prs:
    jan_df = pd.DataFrame(jan_prs)
    jan_agent_repos = set(int(x) for x in jan_df[jan_df['is_agent']]['repo_id'].dropna())
    aidev_agent_repos = set(int(x) for x in aidev_filtered['repo_id'].unique() if pd.notna(x))
    overlap = jan_agent_repos & aidev_agent_repos
    coverage = len(overlap) / max(len(aidev_agent_repos), 1) * 100
    val_report = (
        f"<b>Jan 2025 Validation Sprint</b>\n"
        f"Total PRs collected: {len(jan_df):,}\n"
        f"Agent PRs: {int(jan_df['is_agent'].sum()):,}\n"
        f"Repos with agent PRs (ours): {len(jan_agent_repos):,}\n"
        f"Repos with agent PRs (AIDev): {len(aidev_agent_repos):,}\n"
        f"Repo overlap: {len(overlap):,} ({coverage:.1f}%)"
    )
    print(val_report, flush=True)
    send_telegram(val_report)
else:
    msg = "⚠️ Jan 2025 validation sprint returned no PRs — check logs!"
    print(msg, flush=True)
    send_telegram(msg)

# ════════════════════════════════════════════════════════
# PHASE 1: Main Collection — Dec 2024 to Feb 2026
# ════════════════════════════════════════════════════════
print("\n" + "="*60, flush=True)
print(f"PHASE 1: Main collection ({DATE_FROM} to {DATE_TO})", flush=True)
print("="*60, flush=True)

all_prs, _, _ = _run_pr_collection(repos, DATE_FROM, DATE_TO, CHECKPOINT_FILE, "main")

pr_df = pd.DataFrame(all_prs)
try:
    del all_prs
    import gc
    gc.collect()
except NameError:
    pass

if len(pr_df) == 0:
    msg = "\u26a0 No PRs collected across all repos — cannot proceed. Check errors and re-run."
    print(msg, flush=True)
    send_telegram(msg)
    raise SystemExit(1)

pr_number_map = dict(zip(pr_df['id'], pr_df['number']))
pr_repo_map = dict(zip(pr_df['id'], pr_df['repo_name']))

print(f"Total PRs collected: {len(pr_df):,}")
print()
print("=== Agent vs Human ===")
print(pr_df['agent'].value_counts())
print()
print("=== PR State ===")
print(pr_df['state'].value_counts())

COMMITS_CKPT = f"{CHECKPOINT_DIR}/checkpoint_commits.json"

def fetch_pr_commits(pr_id, repo_name, pr_number, token_idx):
    commits = github_get_all_pages(url, token_idx=token_idx)
    if commits is None:
        print(f"  [ERROR] Failed to fetch commits for {repo_name}#{pr_number}", flush=True)
        return pr_id, [], True
    
    # If commits is [] (not None), it means the PR truly has no commits (rare) 
    # or the first page was empty. We still return True for had_error to be safe
    # if it's unexpected, but at least we didn't print a fetch error.
    if not commits:
        return pr_id, [], True

    results = []
    for c in commits:
        results.append({
            'sha': c['sha'],
            'pr_id': pr_id,
            'author': (c.get('author') or {}).get('login') or (c.get('commit', {}).get('author', {}).get('name', '')),
            'committer': (c.get('committer') or {}).get('login') or (c.get('commit', {}).get('committer', {}).get('name', '')),
            'message': c.get('commit', {}).get('message', '')
        })
    return pr_id, results, False

# Resume
done_pr_ids = set()
all_commits = []
commit_errors = []

if os.path.exists(COMMITS_CKPT):
    try:
        with open(COMMITS_CKPT, 'r') as f:
            ckpt = json.load(f)
        all_commits = ckpt['all_commits']
        commit_errors = ckpt.get('errors', [])
        done_pr_ids = set(ckpt.get('done_pr_ids', []))
        print(f"RESUMED: {len(done_pr_ids)}/{len(pr_df)} PRs done, {len(all_commits)} commits", flush=True)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"WARNING: Commits checkpoint corrupted ({e}), starting fresh.", flush=True)
else:
    print("Starting fresh (no commits checkpoint)", flush=True)

remaining_prs = pr_df[~pr_df['id'].isin(done_pr_ids)].reset_index(drop=True)
print(f"PRs remaining: {len(remaining_prs):,} (of {len(pr_df):,} total)", flush=True)
print(f"Submitting {len(remaining_prs):,} commit fetch tasks...", flush=True)

t_start = time.time()
completed = 0

with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    futures = {}
    for idx, row in enumerate(remaining_prs.itertuples(index=False)):
        token_idx = idx % len(GITHUB_TOKENS)
        fut = executor.submit(fetch_pr_commits, row.id, row.repo_name, row.number, token_idx)
        futures[fut] = row.id
    print(f"All tasks submitted. Collecting results...", flush=True)

    for fut in as_completed(futures):
        completed += 1
        try:
            pr_id, commits, had_error = fut.result()
            all_commits.extend(commits)
            done_pr_ids.add(pr_id)
            if had_error:
                commit_errors.append(pr_id)
        except Exception as e:
            pr_id = futures[fut]
            commit_errors.append(pr_id)
            done_pr_ids.add(pr_id)

        if completed % 200 == 0 or completed == len(remaining_prs):
            elapsed = time.time() - t_start
            rate = completed / max(elapsed, 1) * 3600
            print(f"  Commits: {len(done_pr_ids)}/{len(pr_df)} PRs | {len(all_commits)} commits | {rate:.0f} PRs/hr | Errors: {len(commit_errors)}", flush=True)

            tmp = COMMITS_CKPT + '.tmp'
            with open(tmp, 'w') as f:
                json.dump({
                    'done_pr_ids': [int(x) for x in done_pr_ids],
                    'all_commits': all_commits,
                    'errors': [int(x) for x in commit_errors]
                }, f)
            os.replace(tmp, COMMITS_CKPT)

    # --- Retry Phase for Commits ---
    if commit_errors:
        print(f"\nRetrying {len(commit_errors)} failed PRs...", flush=True)
        retry_prs = pr_df[pr_df['id'].isin(commit_errors)].reset_index(drop=True)
        commit_errors.clear()
        
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            retry_futures = {}
            for idx, row in enumerate(retry_prs.itertuples(index=False)):
                token_idx = idx % len(GITHUB_TOKENS)
                fut = executor.submit(fetch_pr_commits, row.id, row.repo_name, row.number, token_idx)
                retry_futures[fut] = row.id

            completed_retries = 0
            t_retry_start = time.time()
            try:
                for fut in as_completed(retry_futures):
                    completed_retries += 1
                    try:
                        pr_id, commits, had_error = fut.result()
                        all_commits.extend(commits)
                        done_pr_ids.add(pr_id)
                        if had_error:
                            commit_errors.append(pr_id)
                    except Exception:
                        pr_id = retry_futures[fut]
                        commit_errors.append(pr_id)
                        done_pr_ids.add(pr_id)
                        
                    # Save checkpoint periodically during retry
                    if completed_retries % 10000 == 0 or completed_retries == len(retry_prs):
                        elapsed_retry = time.time() - t_retry_start
                        rate_retry = completed_retries / max(elapsed_retry, 1) * 3600
                        print(f"  Retry progress: {completed_retries}/{len(retry_prs)} PRs | {rate_retry:.0f} PRs/hr | Remaining errors in batch so far: {len(commit_errors)}", flush=True)

                        tmp = COMMITS_CKPT + '.tmp'
                        with open(tmp, 'w') as f:
                            json.dump({
                                'done_pr_ids': [int(x) for x in done_pr_ids],
                                'all_commits': all_commits,
                                'errors': [int(x) for x in commit_errors]
                            }, f)
                        os.replace(tmp, COMMITS_CKPT)
            except KeyboardInterrupt:
                print("\n[!] Ctrl+C Intercepted: Forcing emergency checkpoint save for Phase 2 retries before exit...", flush=True)
                executor.shutdown(wait=False, cancel_futures=True)
                tmp = COMMITS_CKPT + '.tmp'
                with open(tmp, 'w') as f:
                    json.dump({
                        'done_pr_ids': [int(x) for x in done_pr_ids],
                        'all_commits': all_commits,
                        'errors': [int(x) for x in commit_errors]
                    }, f)
                os.replace(tmp, COMMITS_CKPT)
                print("[!] Emergency saving complete. Exiting.", flush=True)
                raise
        
        print(f"Retry complete. Remaining errors: {len(commit_errors)}", flush=True)

        tmp = COMMITS_CKPT + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({
                'done_pr_ids': [int(x) for x in done_pr_ids],
                'all_commits': all_commits,
                'errors': [int(x) for x in commit_errors]
            }, f)
        os.replace(tmp, COMMITS_CKPT)

commits_df = pd.DataFrame(all_commits)
try:
    del all_commits
    import gc
    gc.collect()
except NameError:
    pass
elapsed = time.time() - t_start
print(f"\nDone! {len(commits_df)} commits in {elapsed/60:.1f} min. Errors: {len(commit_errors)}", flush=True)
if len(commit_errors) > 0:
    print(f"\u26a0 {len(commit_errors)} PRs had errors \u2014 their commits were NOT collected.", flush=True)

# ════════════════════════════════════════════════════════════════════
# PHASE 3: Commit Details via Local Git Clones
#
# Replaces the old GitHub-API-based commit detail fetching with:
#   1. Mirror-clone each repo locally (parallel, resumable)
#   2. Extract commit metadata + numstat in batch via git log
#   3. Extract file statuses via git diff-tree (batch)
#   4. Optionally extract per-file patches (per-SHA, toggled by INCLUDE_PATCH)
#   5. Write one parquet chunk per repo (no giant in-memory list)
#   6. Merge chunks into final pr_commit_details.parquet
#
# Schema note: author/committer fields contain git names (e.g. "John Doe"),
# not GitHub usernames. pr_commits.parquet already has GitHub logins if needed.
# ════════════════════════════════════════════════════════════════════

# ── Git status letter → GitHub API status word mapping ──
_GIT_STATUS_MAP = {
    'A': 'added', 'M': 'modified', 'D': 'removed',
    'R': 'renamed', 'C': 'copied', 'T': 'changed',
}


def sanitize_repo_path(repo_name):
    """Convert 'owner/repo' → Path('repo_cache/owner__repo.git')."""
    safe = repo_name.replace('/', '__')
    return Path(REPO_CACHE_DIR) / f"{safe}.git"


def clone_or_update_repo(repo_name):
    """
    Mirror-clone a GitHub repo, or fetch updates if already cloned.
    Returns (repo_path: Path, success: bool, error_msg: str).
    """
    repo_path = sanitize_repo_path(repo_name)
    url = f"https://github.com/{repo_name}.git"

    try:
        if repo_path.exists() and (repo_path / 'HEAD').exists():
            # Already cloned — fetch updates AND pull request refs
            result = subprocess.run(
                ['git', '-C', str(repo_path), 'remote', 'update', '--prune'],
                capture_output=True, text=True, timeout=FETCH_TIMEOUT,
            )
            if result.returncode == 0:
                # Explicitly fetch all PR refs to ensure missing SHAs are retrieved
                subprocess.run(
                    ['git', '-C', str(repo_path), 'fetch', 'origin', '+refs/pull/*/head:refs/pull/*/head'],
                    capture_output=True, text=True, timeout=FETCH_TIMEOUT,
                )
            if result.returncode != 0:
                return repo_path, False, f"fetch failed: {result.stderr.strip()}"
            return repo_path, True, ""
        else:
            # Fresh mirror clone
            repo_path.parent.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(
                ['git', 'clone', '--mirror', '--filter=blob:none', url, str(repo_path)],
                capture_output=True, text=True, timeout=CLONE_TIMEOUT,
            )
            if result.returncode == 0:
                # Also fetch PR refs immediately after cloning
                subprocess.run(
                    ['git', '-C', str(repo_path), 'fetch', 'origin', '+refs/pull/*/head:refs/pull/*/head'],
                    capture_output=True, text=True, timeout=FETCH_TIMEOUT,
                )
            if result.returncode != 0:
                return repo_path, False, f"clone failed: {result.stderr.strip()}"
            return repo_path, True, ""
    except subprocess.TimeoutExpired:
        return repo_path, False, "timeout"
    except Exception as e:
        return repo_path, False, str(e)


def _parse_numstat_block(lines):
    """Parse numstat lines into list of (additions, deletions, filename).
    Binary files show as '-\t-\tfilename' → (0, 0, filename)."""
    files = []
    for line in lines:
        if not line.strip():
            continue
        parts = line.split('\t', 2)
        if len(parts) < 3:
            continue
        add_str, del_str, fname = parts
        # Binary files have '-' for both
        add = int(add_str) if add_str != '-' else 0
        dlt = int(del_str) if del_str != '-' else 0
        files.append((add, dlt, fname))
    return files


# Delimiter unlikely to appear in commit messages
_COMMIT_SEP = '---COMMIT_BOUNDARY_8f3a1b---'
_MSG_END    = '---MSG_END_8f3a1b---'


def extract_commit_metadata_and_numstat(repo_path, shas):
    """
    Batch-extract commit metadata and per-file numstat for a list of SHAs.
    Uses `git log --no-walk --stdin` with a structured format.

    Returns dict[sha -> {
        'author': str, 'committer': str, 'message': str,
        'files': [(additions: int, deletions: int, filename: str), ...]
    }]
    Missing SHAs are omitted from the result.
    """
    if not shas:
        return {}

    fmt = (
        f'{_COMMIT_SEP}%n'   # boundary
        '%H%n'                # sha
        '%an%n'               # author name
        '%cn%n'               # committer name
        '%B'                  # full message body
        f'{_MSG_END}'         # end of message marker
    )

    stdin_data = ('\n'.join(shas) + '\n').encode()
    try:
        result = subprocess.run(
            ['git', '-C', str(repo_path), 'log',
             '--no-walk', '--stdin', '--no-renames',
             f'--format={fmt}', '--numstat'],
            input=stdin_data, capture_output=True, timeout=EXTRACT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(f"    TIMEOUT: git log --numstat on {repo_path}", flush=True)
        return {}

    if result.returncode != 0:
        # Some SHAs may not exist — that's okay, we just get fewer results
        pass

    stdout = result.stdout.decode('utf-8', errors='replace')
    commits = {}
    # Split by our delimiter; first chunk is empty or preamble
    blocks = stdout.split(_COMMIT_SEP)
    for block in blocks:
        block = block.strip()
        if not block:
            continue

        # Find the message end marker
        msg_end_pos = block.find(_MSG_END)
        if msg_end_pos == -1:
            continue

        header_and_msg = block[:msg_end_pos]
        numstat_text = block[msg_end_pos + len(_MSG_END):]

        lines = header_and_msg.split('\n', 3)
        if len(lines) < 4:
            continue

        sha = lines[0].strip()
        author = lines[1].strip()
        committer = lines[2].strip()
        message = lines[3].strip()

        files = _parse_numstat_block(numstat_text.strip().split('\n'))
        commits[sha] = {
            'author': author,
            'committer': committer,
            'message': message,
            'files': files,
        }

    return commits


def extract_file_statuses(repo_path, shas):
    """
    Batch-extract per-file status letters (A/M/D/R/C/T) for a list of SHAs.
    Uses `git diff-tree -r --no-commit-id --no-renames --name-status --stdin`.

    Returns dict[sha -> dict[filename -> status_word]].
    """
    if not shas:
        return {}

    # We use diff-tree with -r (recursive) and provide SHAs on stdin.
    # diff-tree with --stdin reads one tree-ish per line and outputs
    # the SHA line followed by file status lines.
    stdin_data = ('\n'.join(shas) + '\n').encode()
    try:
        result = subprocess.run(
            ['git', '-C', str(repo_path), 'diff-tree',
             '-r', '--no-renames', '--name-status', '--stdin',
             '--root'],  # --root shows the diff for root commits too
            input=stdin_data, capture_output=True, timeout=EXTRACT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(f"    TIMEOUT: git diff-tree on {repo_path}", flush=True)
        return {}

    stdout = result.stdout.decode('utf-8', errors='replace')
    statuses = {}
    current_sha = None
    for line in stdout.split('\n'):
        line = line.strip()
        if not line:
            continue
        # diff-tree outputs the commit SHA as a header line (40 hex chars)
        if len(line) >= 40 and all(c in '0123456789abcdef' for c in line[:40]):
            current_sha = line[:40]
            if current_sha not in statuses:
                statuses[current_sha] = {}
            continue
        if current_sha is None:
            continue
        # Status lines: "M\tfilename" or "A\tfilename"
        parts = line.split('\t', 1)
        if len(parts) == 2:
            status_letter = parts[0].strip()
            fname = parts[1].strip()
            # Map git letter to GitHub API word
            statuses[current_sha][fname] = _GIT_STATUS_MAP.get(
                status_letter[0] if status_letter else '', status_letter)

    return statuses


def extract_patches_for_sha(repo_path, sha):
    """
    Extract per-file unified diff patches for a single SHA.
    Returns dict[filename -> patch_text_or_None].

    If INCLUDE_PATCH is False, returns empty dict.
    Patches exceeding MAX_PATCH_BYTES are stored as None.
    """
    if not INCLUDE_PATCH:
        return {}

    try:
        result = subprocess.run(
            ['git', '-C', str(repo_path), 'log', '-1',
             '--no-renames', '--format=', '-p', sha],
            capture_output=True, timeout=EXTRACT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return {}

    if result.returncode != 0:
        return {}

    stdout = result.stdout.decode('utf-8', errors='replace')
    patches = {}
    # Split by diff headers: "diff --git a/path b/path"
    diff_blocks = _re.split(r'^diff --git ', stdout, flags=_re.MULTILINE)
    for block in diff_blocks:
        if not block.strip():
            continue
        # First line is "a/path b/path\n..."
        first_line_end = block.find('\n')
        if first_line_end == -1:
            continue
        header = block[:first_line_end]
        # Extract filename from "a/path b/path" — use the b/ side
        parts = header.split(' b/', 1)
        if len(parts) < 2:
            continue
        fname = parts[1].strip()
        # The patch text is everything after the header line
        patch_text = block[first_line_end + 1:]
        if len(patch_text.encode('utf-8', errors='replace')) > MAX_PATCH_BYTES:
            patches[fname] = None
        else:
            patches[fname] = patch_text
    return patches


def fetch_commit_from_api(repo_name, sha, token_idx):
    """Fallback: fetch a single commit's details via GitHub API when local git fails."""
    url = f"https://api.github.com/repos/{repo_name}/commits/{sha}"
    data = github_get(url, token_idx=token_idx)
    if not data:
        return None

    # Format into a structure similar to what extract_commit_metadata returns
    commit_data = data.get('commit', {})
    stats = data.get('stats', {})
    
    files = []
    for f in data.get('files', []):
        add = f.get('additions', 0)
        dlt = f.get('deletions', 0)
        fname = f.get('filename')
        status = f.get('status', 'modified')
        patch = f.get('patch') if INCLUDE_PATCH else None
        files.append({
            'filename': fname,
            'status': status,
            'additions': add,
            'deletions': dlt,
            'changes': add + dlt,
            'patch': patch
        })

    return {
        'sha': sha,
        'author': commit_data.get('author', {}).get('name', ''),
        'committer': commit_data.get('committer', {}).get('name', ''),
        'message': commit_data.get('message', ''),
        'commit_stats_total': stats.get('total', 0),
        'commit_stats_additions': stats.get('additions', 0),
        'commit_stats_deletions': stats.get('deletions', 0),
        'files_detailed': files # special field for API results
    }


def process_repo_details(repo_name, shas, sha_to_pr_ids, repo_path, token_idx=0):
    """
    Extract commit details for SHAs in a single repo in batches.
    
    Uses local git by default; falls back to GitHub API if SHAs are missing locally.
    Yields (detail_rows: list[dict], missing_shas: list[str]) for each batch.
    detail_rows has one row per (sha, pr_id, filename) combination.
    """
    total_shas = len(shas)
    processed_shas = 0
    # Process SHAs in batches
    for batch_start in range(0, total_shas, SHA_BATCH_SIZE):
        batch = shas[batch_start:batch_start + SHA_BATCH_SIZE]
        detail_rows = []
        missing_shas = []

        # Batch 1: metadata + numstat
        meta = extract_commit_metadata_and_numstat(repo_path, batch)
        # Batch 2: file statuses
        statuses = extract_file_statuses(repo_path, batch)

        for sha in batch:
            if sha not in meta:
                # API Fallback for missing SHAs
                api_info = fetch_commit_from_api(repo_name, sha, token_idx)
                if not api_info:
                    missing_shas.append(sha)
                    continue
                
                # API Success — map to rows
                base = {
                    'sha': sha,
                    'author': api_info['author'],
                    'committer': api_info['committer'],
                    'message': api_info['message'],
                    'commit_stats_total': api_info['commit_stats_total'],
                    'commit_stats_additions': api_info['commit_stats_additions'],
                    'commit_stats_deletions': api_info['commit_stats_deletions'],
                }
                pr_ids = sha_to_pr_ids.get(sha, [None])
                if not api_info['files_detailed']:
                    for pid in pr_ids:
                        detail_rows.append({
                            **base, 'pr_id': pid,
                            'filename': None, 'status': None, 'additions': None,
                            'deletions': None, 'changes': None, 'patch': None
                        })
                else:
                    for f in api_info['files_detailed']:
                        for pid in pr_ids:
                            detail_rows.append({
                                **base, 'pr_id': pid,
                                'filename': f['filename'], 'status': f['status'],
                                'additions': f['additions'], 'deletions': f['deletions'],
                                'changes': f['changes'], 'patch': f['patch']
                            })
                continue

            info = meta[sha]
            files = info['files']  # [(add, del, fname), ...]
            sha_statuses = statuses.get(sha, {})

            # Per-SHA patch extraction (only if INCLUDE_PATCH is True)
            patches = extract_patches_for_sha(repo_path, sha) if INCLUDE_PATCH else {}

            # Compute commit-level stats
            total_add = sum(f[0] for f in files)
            total_del = sum(f[1] for f in files)
            total_chg = total_add + total_del

            base = {
                'sha': sha,
                'author': info['author'],
                'committer': info['committer'],
                'message': info['message'],
                'commit_stats_total': total_chg,
                'commit_stats_additions': total_add,
                'commit_stats_deletions': total_del,
            }

            pr_ids = sha_to_pr_ids.get(sha, [])
            if not pr_ids:
                # Shouldn't happen, but be safe
                pr_ids = [None]

            if not files:
                # Commit with no file changes (e.g. empty or merge commit)
                for pid in pr_ids:
                    detail_rows.append({
                        **base, 'pr_id': pid,
                        'filename': None, 'status': None,
                        'additions': None, 'deletions': None,
                        'changes': None, 'patch': None,
                    })
            else:
                for add, dlt, fname in files:
                    status = sha_statuses.get(fname, 'modified')
                    patch = patches.get(fname) if INCLUDE_PATCH else None
                    for pid in pr_ids:
                        detail_rows.append({
                            **base, 'pr_id': pid,
                            'filename': fname,
                            'status': status,
                            'additions': add,
                            'deletions': dlt,
                            'changes': add + dlt,
                            'patch': patch,
                        })

        processed_shas += len(batch)
        if processed_shas % 10000 == 0 or processed_shas == total_shas:
            print(f"    [{repo_name}] Extracted {processed_shas:,}/{total_shas:,} SHAs...", flush=True)

        yield detail_rows, missing_shas


def _save_details_checkpoint(ckpt_path, done_repos, error_repos, all_missing_shas):
    """Atomically save a lightweight checkpoint (metadata only, no data rows)."""
    tmp = ckpt_path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({
            'done_repos': sorted(done_repos),
            'error_repos': sorted(error_repos),
            'missing_shas': sorted(all_missing_shas),
        }, f)
    os.replace(tmp, ckpt_path)


def merge_detail_chunks(chunk_dir, output_path):
    """Read all per-repo parquet chunks and merge into a single parquet file (streaming to save RAM)."""
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    chunk_files = sorted(Path(chunk_dir).glob('*.parquet'))
    if not chunk_files:
        print("  No detail chunk files found — writing empty parquet.", flush=True)
        pd.DataFrame().to_parquet(output_path, index=False)
        return 0, pd.DataFrame()

    print(f"  Merging {len(chunk_files)} chunk files sequentially to save RAM...", flush=True)
    
    # Use PyArrow Dataset to discover unified schema and stream
    dataset = ds.dataset(chunk_dir, format="parquet")
    schema = dataset.schema
    
    total_rows = 0
    with pq.ParquetWriter(output_path, schema) as writer:
        # Scanner streams in memory-friendly batches. We explicitly force
        # small batches so PyArrow doesn't try to load massive 1GB fragments.
        for batch in dataset.to_batches(batch_size=50000):
            writer.write_batch(batch)
            total_rows += batch.num_rows

    sample_df = pd.DataFrame()
    if total_rows > 0:
        try:
            # Grab a small sample from the first file for console print output
            sample_df = pd.read_parquet(chunk_files[0]).head(20)
        except Exception:
            pass

    print(f"  Merged: {total_rows:,} rows → {output_path}", flush=True)
    return total_rows, sample_df


# ── Phase 3 execution ──────────────────────────────────────────────
print("\n" + "="*60, flush=True)
print("PHASE 3: Commit details via local git clones", flush=True)
print("="*60, flush=True)

if len(commits_df) == 0:
    print("\u26a0 No commits to fetch details for. Skipping.")
    commit_details_df = pd.DataFrame()
else:
    os.makedirs(REPO_CACHE_DIR, exist_ok=True)
    os.makedirs(DETAILS_CHUNK_DIR, exist_ok=True)

    # Build sha → [pr_id, ...] mapping (preserves multi-PR relationships)
    sha_to_pr_ids = defaultdict(list)
    for row in commits_df.itertuples(index=False):
        sha_to_pr_ids[row.sha].append(row.pr_id)

    # Build pr_id → repo_name mapping
    _pr_repo_local = dict(zip(pr_df['id'], pr_df['repo_name']))

    # Group unique SHAs by repo
    repo_shas = defaultdict(list)
    seen_shas = set()
    for sha, pr_ids in sha_to_pr_ids.items():
        if sha in seen_shas:
            continue
        seen_shas.add(sha)
        # Find repo from any associated PR
        repo_name = None
        for pid in pr_ids:
            repo_name = _pr_repo_local.get(pid)
            if repo_name:
                break
        if repo_name:
            repo_shas[repo_name].append(sha)
        else:
            print(f"  WARNING: No repo found for SHA {sha} (pr_ids={pr_ids})", flush=True)

    total_unique_shas = sum(len(v) for v in repo_shas.values())
    total_repos = len(repo_shas)
    print(f"Unique SHAs: {total_unique_shas:,} across {total_repos:,} repos", flush=True)
    print(f"Config: CLONE_WORKERS={CLONE_WORKERS}, INCLUDE_PATCH={INCLUDE_PATCH}, "
          f"MAX_PATCH_BYTES={MAX_PATCH_BYTES:,}, SHA_BATCH_SIZE={SHA_BATCH_SIZE}", flush=True)

    # Load checkpoint
    done_repos = set()
    error_repos = set()
    all_missing_shas = set()
    if os.path.exists(DETAILS_CKPT_FILE):
        try:
            with open(DETAILS_CKPT_FILE, 'r') as f:
                ckpt = json.load(f)
            done_repos = set(ckpt.get('done_repos', []))
            error_repos = set(ckpt.get('error_repos', []))
            all_missing_shas = set(ckpt.get('missing_shas', []))
            print(f"RESUMED: {len(done_repos)}/{total_repos} repos done, "
                  f"{len(error_repos)} errors, {len(all_missing_shas)} missing SHAs", flush=True)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"WARNING: Details checkpoint corrupted ({e}), starting fresh.", flush=True)

    remaining_repos = [r for r in repo_shas if r not in done_repos]
    print(f"Repos remaining: {len(remaining_repos):,}", flush=True)
    print(f"DELETE_AFTER_EXTRACT={DELETE_AFTER_EXTRACT} — "
          f"{'clones are deleted after processing to save disk' if DELETE_AFTER_EXTRACT else 'clones are kept in repo_cache/'}",
          flush=True)

    # ── Pipeline: clone → extract → write chunk → delete (parallel) ──
    # Up to CLONE_WORKERS repos are processed concurrently.
    # With DELETE_AFTER_EXTRACT=True, at most CLONE_WORKERS clones exist on disk.
    print(f"\nProcessing {len(remaining_repos)} repos ({CLONE_WORKERS} parallel workers)...", flush=True)
    _progress["phase"] = "local-git-details"
    _progress["repos_total"] = total_repos
    _progress["repos_done"] = len(done_repos)
    _progress["commits_done"] = 0

    send_telegram(
        f"<b>Phase 3 started: commit details via local git</b>\n"
        f"Repos remaining: {len(remaining_repos):,} (of {total_repos:,})\n"
        f"Unique SHAs: {total_unique_shas:,}\n"
        f"Workers: {CLONE_WORKERS}\n"
        f"INCLUDE_PATCH={INCLUDE_PATCH}, DELETE_AFTER_EXTRACT={DELETE_AFTER_EXTRACT}"
    )

    detail_t = time.time()
    total_rows_written = 0
    repos_processed = 0
    clone_ok_count = 0
    clone_fail_count = 0
    _details_lock = threading.Lock()

    def _process_one_repo(repo_name, token_idx):
        """Clone → extract → write chunk → delete for a single repo (thread worker)."""
        shas = repo_shas[repo_name]
        repo_path = sanitize_repo_path(repo_name)

        # Step 1: Clone or update
        repo_path, ok, err = clone_or_update_repo(repo_name)
        if not ok:
            return (repo_name, 0, list(shas), err, False)

        # Step 2: Extract & Step 3: Write per-repo parquet chunks in batches
        total_missing = []
        num_rows_total = 0
        try:
            for i, (rows, missing) in enumerate(process_repo_details(repo_name, shas, sha_to_pr_ids, repo_path)):
                total_missing.extend(missing)
                if rows:
                    chunk_name = repo_name.replace('/', '__') + f'_part{i}.parquet'
                    chunk_path = Path(DETAILS_CHUNK_DIR) / chunk_name
                    pd.DataFrame(rows).to_parquet(str(chunk_path), index=False)
                    num_rows_total += len(rows)
        except Exception as e:
            if DELETE_AFTER_EXTRACT and repo_path.exists():
                shutil.rmtree(repo_path, ignore_errors=True)
            return (repo_name, num_rows_total, total_missing + list(shas), str(e), True)

        # Step 4: Delete clone to free disk
        if DELETE_AFTER_EXTRACT and repo_path.exists():
            shutil.rmtree(repo_path, ignore_errors=True)

        return (repo_name, num_rows_total, total_missing, None, True)

    with ThreadPoolExecutor(max_workers=CLONE_WORKERS) as executor:
        futures = {}
        for i, rn in enumerate(remaining_repos):
            t_idx = i % len(GITHUB_TOKENS)
            futures[executor.submit(_process_one_repo, rn, t_idx)] = rn

        for fut in as_completed(futures):
            repos_processed += 1
            rn, num_rows, missing_or_shas, err_msg, clone_ok = fut.result()

            with _details_lock:
                if err_msg is not None:
                    if not clone_ok:
                        clone_fail_count += 1
                        print(f"  CLONE FAIL [{repos_processed}/{len(remaining_repos)}] "
                              f"{rn}: {err_msg}", flush=True)
                    else:
                        print(f"  EXTRACT ERROR [{repos_processed}/{len(remaining_repos)}] "
                              f"{rn}: {err_msg}", flush=True)
                    for sha in missing_or_shas:
                        all_missing_shas.add(sha)
                    error_repos.add(rn)
                else:
                    clone_ok_count += 1
                    all_missing_shas.update(missing_or_shas)
                    total_rows_written += num_rows

                done_repos.add(rn)
                _progress["repos_done"] = len(done_repos)
                _progress["commits_done"] = total_rows_written

                # Checkpoint + progress every 50 repos or at the end
                if repos_processed % 50 == 0 or repos_processed == len(remaining_repos):
                    elapsed = time.time() - detail_t
                    rate = repos_processed / max(elapsed, 1) * 3600
                    print(f"  Details: {len(done_repos)}/{total_repos} repos | "
                          f"{total_rows_written:,} rows | {len(all_missing_shas)} missing | "
                          f"{rate:.0f} repos/hr | clones: {clone_ok_count} OK {clone_fail_count} fail",
                          flush=True)
                    _save_details_checkpoint(DETAILS_CKPT_FILE, done_repos, error_repos, all_missing_shas)

                    # Telegram update every 100 repos
                    if repos_processed % 100 == 0:
                        send_telegram(
                            f"<b>Details progress</b>\n"
                            f"Repos: {len(done_repos)}/{total_repos}\n"
                            f"Missing SHAs: {len(all_missing_shas):,}"
                        )

    # --- Retry Phase for git details ---
    if error_repos:
        print(f"\nRetrying {len(error_repos)} failed repos...", flush=True)
        repos_to_retry = list(error_repos)
        error_repos.clear() 
        _progress["phase"] = "local-git-details-retry"
        
        with ThreadPoolExecutor(max_workers=CLONE_WORKERS) as executor:
            retry_futures = {}
            for i, rn in enumerate(repos_to_retry):
                t_idx = i % len(GITHUB_TOKENS)
                retry_futures[executor.submit(_process_one_repo, rn, t_idx)] = rn

            for fut in as_completed(retry_futures):
                rn, num_rows, missing_or_shas, err_msg, clone_ok = fut.result()
                with _details_lock:
                    if err_msg is not None:
                        if not clone_ok:
                            print(f"  CLONE FAIL (Retry) {rn}: {err_msg}", flush=True)
                        else:
                            print(f"  EXTRACT ERROR (Retry) {rn}: {err_msg}", flush=True)
                        for sha in missing_or_shas:
                            all_missing_shas.add(sha)
                        error_repos.add(rn)
                    else:
                        all_missing_shas.update(missing_or_shas)
                        total_rows_written += num_rows
                        done_repos.add(rn)
        
        _save_details_checkpoint(DETAILS_CKPT_FILE, done_repos, error_repos, all_missing_shas)
        print(f"Retry complete. Remaining errors: {len(error_repos)}", flush=True)

    # Final checkpoint
    _save_details_checkpoint(DETAILS_CKPT_FILE, done_repos, error_repos, all_missing_shas)

    detail_elapsed = time.time() - detail_t
    print(f"\nDetail extraction done in {detail_elapsed/60:.1f} min", flush=True)
    print(f"  Repos processed: {len(done_repos)} ({len(error_repos)} errors)", flush=True)
    print(f"  Missing SHAs: {len(all_missing_shas)}", flush=True)

    # ── Step C: Merge per-repo chunks into final parquet ──
    commit_details_total, commit_details_sample = merge_detail_chunks(
        DETAILS_CHUNK_DIR,
        f"{CHECKPOINT_DIR}/pr_commit_details.parquet",
    )
    print(f"  Final commit details: {commit_details_total:,} rows", flush=True)

OUTPUT_DIR = "data_dec2024_feb2026"
os.makedirs(OUTPUT_DIR, exist_ok=True)

agent_pr_df = pr_df[pr_df['is_agent'] == True].copy()
human_pr_df = pr_df[pr_df['is_agent'] == False].copy()

# Save all tables
pr_df.to_parquet(f"{OUTPUT_DIR}/all_pull_requests.parquet", index=False)
agent_pr_df.to_parquet(f"{OUTPUT_DIR}/agent_pull_requests.parquet", index=False)
human_pr_df.to_parquet(f"{OUTPUT_DIR}/human_pull_requests.parquet", index=False)
commits_df.to_parquet(f"{OUTPUT_DIR}/pr_commits.parquet", index=False)
if os.path.abspath(CHECKPOINT_DIR) != os.path.abspath(OUTPUT_DIR):
    shutil.copy(f"{CHECKPOINT_DIR}/pr_commit_details.parquet", f"{OUTPUT_DIR}/pr_commit_details.parquet")

print("All 5 files saved!\n")
for f in sorted(os.listdir(OUTPUT_DIR)):
    if f.endswith('.parquet'):
        size = os.path.getsize(f"{OUTPUT_DIR}/{f}") / (1024*1024)
        print(f"  {f}: {size:.2f} MB")

print("=" * 60)
print(f"  FULL COLLECTION SUMMARY — Dec 2024 to Feb 2026 (15 months)")
print("=" * 60)
print()
print(f"Date range:          {DATE_FROM} to {DATE_TO}")
print(f"Total PRs:           {len(pr_df):,}")
print(f"  Agent PRs:         {len(agent_pr_df):,}")
print(f"  Human PRs:         {len(human_pr_df):,}")
print(f"Repos with data:     {pr_df['repo_name'].nunique():,}")
print(f"Total commits:       {len(commits_df):,}")
print(f"Commit detail rows:  {commit_details_total:,}")
print()

print("=== Agent PR Breakdown ===")
print(agent_pr_df['agent'].value_counts() if len(agent_pr_df) > 0 else "No agent PRs")
print()

if len(agent_pr_df) > 0:
    agent_merged = agent_pr_df['merged_at'].notna().sum()
    print(f"Agent merge rate: {agent_merged}/{len(agent_pr_df)} = {agent_merged/len(agent_pr_df)*100:.1f}%")
if len(human_pr_df) > 0:
    human_merged = human_pr_df['merged_at'].notna().sum()
    print(f"Human merge rate: {human_merged}/{len(human_pr_df)} = {human_merged/len(human_pr_df)*100:.1f}%")

print()
print("=== Schema Match with AIDev ===")
tables = {
    'all_pull_request': pr_df,
    'pr_commits': commits_df,
    'pr_commit_details': commit_details_sample,
}
for name, df in tables.items():
    if name == 'pr_commit_details':
        print(f"  {name:25s} {commit_details_total:>8,} rows  cols={list(df.columns)}")
    else:
        print(f"  {name:25s} {len(df):>8,} rows  cols={list(df.columns)}")

# Sample from each table
for name, df in tables.items():
    print(f"\n{'='*60}")
    print(f"  {name} — sample (2 rows)")
    print(f"{'='*60}")
    if len(df) > 0:
        print(df.sample(min(2, len(df))).to_string())
    else:
        print("  (empty)")

# Export sample CSVs (20 rows each) for quick inspection
SAMPLE_DIR = f"{OUTPUT_DIR}/samples"
os.makedirs(SAMPLE_DIR, exist_ok=True)

samples = {
    'all_pull_requests': pr_df,
    'agent_pull_requests': agent_pr_df,
    'human_pull_requests': human_pr_df,
    'pr_commits': commits_df,
    'pr_commit_details': commit_details_sample,
}

for name, df in samples.items():
    n = min(20, len(df))
    sample = df.sample(n, random_state=42) if n > 0 else df
    path = f"{SAMPLE_DIR}/{name}_sample.csv"
    sample.to_csv(path, index=False)
    print(f"  {name}_sample.csv  ({n} rows, {len(df.columns)} cols)")

print(f"\nAll sample CSVs saved to {SAMPLE_DIR}/")

# Final completion notification
_final_msg = (
    f"<b>\u2705 Collection complete!</b>\n"
    f"PRs: {len(pr_df):,} (agent: {len(agent_pr_df):,}, human: {len(human_pr_df):,})\n"
    f"Commits: {len(commits_df):,}\n"
    f"Detail rows: {commit_details_total:,}\n\n"
    f"<b>Final token status:</b>\n{_query_token_status()}"
)
print(_final_msg, flush=True)
send_telegram(_final_msg)