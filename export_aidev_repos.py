import pandas as pd
import sys
import re

def extract_repo_from_url(url):
    """Extract owner/repo from a GitHub URL."""
    if not isinstance(url, str):
        return None
    # Matches github.com/owner/repo
    match = re.search(r'github\.com/([^/]+/[^/]+)', url)
    if match:
        # Avoid including /pull/... or other suffixes
        repo = match.group(1).split('/pull/')[0].split('/tree/')[0].split('/blob/')[0]
        # Ensure it only has owner/repo
        parts = repo.split('/')
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
    return None

def clean_github_url(url):
    """Remove /pull/... or other suffixes from GitHub URLs."""
    if not isinstance(url, str):
        return url
    # Matches up to the end of owner/repo
    match = re.search(r'(https?://github\.com/[^/]+/[^/]+)', url)
    if match:
        return match.group(1).rstrip('/') + '/'
    return url

def export_aidev_repos(output_csv="aidev_repositories.csv"):
    print("Loading AIDev PR data from Hugging Face...")
    
    try:
        # Load PRs with additional repo data columns
        aidev_prs = pd.read_parquet(
            "hf://datasets/hao-li/AIDev/all_pull_request.parquet",
            columns=['repo_id', 'repo_url', 'html_url', 'id']
        )
        print(f"Loaded {len(aidev_prs):,} PRs from all_pull_request.parquet")
        
        # Count PRs per repo_id and keep the first occurrence of url metadata
        repo_counts = aidev_prs.groupby('repo_id').size().reset_index(name='pr_count')
        repo_urls = aidev_prs.groupby('repo_id')[['repo_url', 'html_url']].first().reset_index()
        repo_agg = pd.merge(repo_counts, repo_urls, on='repo_id')
        
        unique_repos_in_prs = set(repo_agg['repo_id'])
        print(f"Found PRs in {len(unique_repos_in_prs):,} unique repositories.")
        
        print("Loading AIDev Repository data...")
        # Load Repositories for names and metadata
        repo_meta_df = pd.read_parquet(
            "hf://datasets/hao-li/AIDev/repository.parquet", 
            columns=['id', 'url', 'license', 'full_name', 'language', 'forks', 'stars']
        )
        repos_in_meta = set(repo_meta_df['id'])
        print(f"Loaded {len(repo_meta_df):,} repositories from repository.parquet")
        
        # Calculate overlap and stats
        overlap = unique_repos_in_prs.intersection(repos_in_meta)
        only_in_prs = unique_repos_in_prs - repos_in_meta
        
        # Merge counts with names
        repo_meta_df = repo_meta_df.rename(columns={'id': 'repo_id'})
        merged_df = pd.merge(repo_agg, repo_meta_df, on='repo_id', how='left')
        
        # Add metadata flag
        merged_df['in_repository_metadata'] = merged_df['repo_id'].isin(repos_in_meta)
        
        # Derive name from URL if missing
        def get_final_name(row):
            if pd.notna(row['full_name']) and row['full_name'] != 'Unknown':
                return row['full_name']
            
            # Try html_url first, then repo_url
            for url_field in ['html_url', 'repo_url']:
                extracted = extract_repo_from_url(row[url_field])
                if extracted:
                    return extracted
            return 'Unknown'

        print("Refining repository names and cleaning URLs...")
        merged_df['repository'] = merged_df.apply(get_final_name, axis=1)
        
        # Clean html_url
        merged_df['html_url'] = merged_df['html_url'].apply(clean_github_url)
        
        # Stats on name recovery
        recovered = len(merged_df[(merged_df['repository'] != 'Unknown') & (~merged_df['in_repository_metadata'])])
        
        print("\n" + "="*50)
        print("COLLECTIVE STATISTICS & INCONSISTENCY HIGHLIGHTS")
        print("="*50)
        print(f"Repos in repository.parquet:          {len(repos_in_meta):,}")
        print(f"Unique repos in PRs:                  {len(unique_repos_in_prs):,}")
        print(f"Overlap (found in both):              {len(overlap):,}")
        print(f"Repos in PRs missing from metadata:   {len(only_in_prs):,}")
        print(f"Names recovered from URLs:            {recovered:,}")
        
        overlap_pct = (len(overlap) / len(unique_repos_in_prs) * 100) if len(unique_repos_in_prs) > 0 else 0
        print(f"Metadata Coverage for PR Repos:       {overlap_pct:.2f}%")
        print("="*50 + "\n")
        
        # Organize final columns
        final_df = merged_df[[
            'repo_id', 'repository', 'pr_count', 'in_repository_metadata', 
            'language', 'stars', 'forks', 'license', 'repo_url', 'html_url'
        ]]
        
        # Sort by PR count descending
        final_df = final_df.sort_values(by=['pr_count', 'repository'], ascending=[False, True])
        
        print(f"Exporting to {output_csv}...")
        final_df.to_csv(output_csv, index=False)
        print(f"Successfully exported {len(final_df):,} repositories to {output_csv}")
        
        print("\nTop 10 repositories by PR count:")
        print(final_df[['repository', 'repo_id', 'pr_count', 'in_repository_metadata']].head(10).to_string(index=False))
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    export_aidev_repos()
