# Agentic-PR-A-Large-Scale-Dataset-of-AI-Generated-and-Human-Pull-Requests-on-GitHub
A large-scale dataset of AI-generated and human pull requests collected from GitHub, including PR metadata, commits, diffs, and repository information.
Efficient, multi-threaded toolkit for collecting large-scale pull request and commit data from GitHub repositories, with a specific focus on analyzing AI-generated PRs (e.g., from Devin, Copilot, Cursor, Claude Code) versus human contributions.

## Features

- **High-Speed Collection**: Uses parallel threads and transparent GitHub API token rotation to maximize collection speed while respecting rate limits.
- **Resilient Checkpointing**: Automatically saves checkpoints during large collection runs, allowing you to seamlessly restart or recover from network interruptions.
- **Hot-Reload Setup**: Automatically detect changes in the `.env` file (like adding a new GitHub token) without restarting the running script.
- **Telegram Heartbeat Monitoring**: Optional periodic progress updates and alerts via a Telegram bot.
- **Local Mirroring**: Downloads commits via local Git mirrors (`repo_cache`) and extracts patch data securely.

## Prerequisites

- Python 3.8+
- [Git](https://git-scm.com/downloads) installed and available in the system PATH.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/mabujadallah/Agentic-PR-A-Large-Scale-Dataset-of-AI-Generated-and-Human-Pull-Requests-on-GitHub.git
   cd Agentic-PR-A-Large-Scale-Dataset-of-AI-Generated-and-Human-Pull-Requests-on-GitHub
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and fill in your GitHub tokens separated by commas:
   ```
   GITHUB_TOKENS=ghp_YOUR_TOKEN_1,ghp_YOUR_TOKEN_2
   ```

## Usage

Simply run the collector script:

```bash
python collector.py
```

The script will automatically discover required repositories using Hugging Face datasets (`hao-li/AIDev`), then begin fetching down PR metadata and commits locally into `checkpoint_*.json` and local dataset directories.

## License

This source code is licensed as outlined in the [LICENSE](LICENSE) file.
