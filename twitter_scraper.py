"""
X (Twitter) Data Scraper
This script scrapes data from X (Twitter) using the official API.

Requirements:
- tweepy library for X API access
- python-dotenv for environment variables
- pandas for data handling

Setup:
1. Copy .env.example to .env
2. Add your X API credentials to .env
3. Run: pip install -r requirements.txt
"""

import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import tweepy

# Load environment variables
load_dotenv()

class TwitterScraper:
    """Class to handle X (Twitter) data scraping (Free tier aware)"""
    
    def __init__(self):
        self.bearer_token = os.getenv('X_BEARER_TOKEN')
        self.api_key = os.getenv('X_API_KEY')
        self.api_secret = os.getenv('X_API_SECRET')
        self.access_token = os.getenv('X_ACCESS_TOKEN')
        self.access_token_secret = os.getenv('X_ACCESS_TOKEN_SECRET')
        
        self.client = tweepy.Client(
            bearer_token=self.bearer_token,
            consumer_key=self.api_key,
            consumer_secret=self.api_secret,
            access_token=self.access_token,
            access_token_secret=self.access_token_secret,
            wait_on_rate_limit=True
        )
        
        self.output_dir = Path(os.getenv('OUTPUT_DIR', 'data'))
        self.output_dir.mkdir(exist_ok=True)

    def _counts_endpoint(self, query):
        """Try recent counts endpoint (may not be available in Free tier)."""
        try:
            resp = self.client.get_recent_tweets_count(query=query, granularity='day')
            if resp.data:
                total = sum(row['tweet_count'] for row in resp.data)
                return {"supported": True, "total_estimated": total, "breakdown": resp.data}
            return {"supported": True, "total_estimated": 0, "breakdown": []}
        except Exception as e:
            return {"supported": False, "error": str(e)}

    # --- Usage tracking (local, best-effort) ---
    def _usage_path(self):
        return self.output_dir / "usage.json"

    def _usage_key(self):
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m")  # e.g., 2025-11

    def load_usage(self):
        p = self._usage_path()
        if p.exists():
            try:
                return json.load(open(p, "r", encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save_usage(self, usage):
        p = self._usage_path()
        with open(p, "w", encoding="utf-8") as f:
            json.dump(usage, f, ensure_ascii=False, indent=2)

    def add_usage(self, count, monthly_cap=1500):
        usage = self.load_usage()
        key = self._usage_key()
        month_data = usage.get(key, {"retrieved": 0, "cap": monthly_cap})
        month_data["retrieved"] = int(month_data.get("retrieved", 0)) + int(count)
        month_data["cap"] = monthly_cap
        usage[key] = month_data
        self.save_usage(usage)

    def remaining_quota(self, monthly_cap=1500):
        usage = self.load_usage()
        key = self._usage_key()
        used = int(usage.get(key, {}).get("retrieved", 0))
        return max(0, monthly_cap - used)

    # --- Helper to build query with optional filters ---
    def build_query(self, base, lang=None, exclude_retweets=False, exclude_replies=False):
        parts = [base]
        if exclude_retweets:
            parts.append("-is:retweet")
        if exclude_replies:
            parts.append("-is:reply")
        if lang:
            parts.append(f"lang:{lang}")
        return " ".join(parts)

    def search_tweets(
        self,
        query,
        max_results=100,
        paginate=False,
        total_limit=300,
        start_time=None,
        end_time=None,
        since_id=None
    ):
        """
        Search tweets within a window; paginate until total_limit or no more pages.
        """
        max_results = min(max_results, 100)
        collected = []
        pages = 0
        next_token = None

        # Informational only (might be unavailable in Free tier)
        counts_info = self._counts_endpoint(query)
        if counts_info.get("supported"):
            print(f"[INFO] Counts endpoint estimate: {counts_info['total_estimated']} (7-day window)")
        else:
            print("[INFO] Counts endpoint not available. Using pagination.")

        while True:
            try:
                resp = self.client.search_recent_tweets(
                    query=query,
                    max_results=max_results,
                    next_token=next_token,
                    start_time=start_time,
                    end_time=end_time,
                    since_id=since_id,
                    tweet_fields=['created_at', 'public_metrics', 'author_id', 'lang'],
                    expansions=['author_id'],
                    user_fields=['username', 'name', 'verified', 'public_metrics']
                )
            except tweepy.TweepyException as e:
                print(f"[ERROR] search_recent_tweets: {e}")
                break

            pages += 1
            users_map = {u.id: u for u in (resp.includes.get('users', []) if resp.includes else [])}

            if resp.data:
                for t in resp.data:
                    u = users_map.get(t.author_id)
                    collected.append({
                        "tweet_id": t.id,
                        "text": t.text.replace("\n", " ").strip(),
                        "created_at": t.created_at,
                        "author_id": t.author_id,
                        "author_username": getattr(u, "username", None),
                        "author_name": getattr(u, "name", None),
                        "author_verified": getattr(u, "verified", None),
                        "retweet_count": t.public_metrics.get('retweet_count'),
                        "reply_count": t.public_metrics.get('reply_count'),
                        "like_count": t.public_metrics.get('like_count'),
                        "quote_count": t.public_metrics.get('quote_count'),
                        "language": t.lang
                    })
            else:
                print("[INFO] Page returned no data.")
                break

            meta = resp.meta or {}
            next_token = meta.get("next_token")

            print(f"[PAGE {pages}] collected so far: {len(collected)} "
                  f"(this page: {len(resp.data)})")

            if not paginate:
                break
            if not next_token:
                print("[INFO] No more pages.")
                break
            if len(collected) >= total_limit:
                print("[INFO] Reached total_limit cap.")
                break

        df = pd.DataFrame(collected)
        meta_out = {
            "pages": pages,
            "collected": len(df),
            "query": query,
            "counts_endpoint": counts_info,
            "pagination_used": paginate,
            "total_limit": total_limit,
            "start_time": start_time.isoformat() if isinstance(start_time, datetime) else str(start_time),
            "end_time": end_time.isoformat() if isinstance(end_time, datetime) else str(end_time),
            "since_id": since_id
        }
        return df, meta_out

    def save_or_append_csv(self, df, path: Path):
        if df.empty:
            print("No data to save.")
            return 0
        if path.exists():
            old = pd.read_csv(path)
            merged = pd.concat([old, df], ignore_index=True)
            merged.drop_duplicates(subset=["tweet_id"], inplace=True)
        else:
            merged = df
        # Sort newest first if created_at exists
        if "created_at" in merged.columns:
            merged["created_at"] = pd.to_datetime(merged["created_at"], errors="coerce")
            merged.sort_values("created_at", ascending=False, inplace=True)
        merged.to_csv(path, index=False, encoding="utf-8")
        print(f"[SAVE] CSV -> {path} (rows={len(merged)})")
        return len(df)

def main():
    print("X (Twitter) Data Scraper - Free Tier")
    print("=" * 52)
    print("Collecting 'COPASA' from now back to 7 days or until limit.")
    print("=" * 52)

    scraper = TwitterScraper()

    # Window: last 7 days (explicit)
    now = datetime.now(timezone.utc)
    start_7d = now - timedelta(days=7)

    # Monthly cap (configurable via env)
    monthly_cap = int(os.getenv("X_MONTHLY_CAP", "1500"))
    remaining = scraper.remaining_quota(monthly_cap)
    if remaining <= 0:
        print(f"[STOP] Monthly cap reached (cap={monthly_cap}).")
        return

    # Build query focused only on COPASA (adjust filters if needed)
    query = scraper.build_query("COPASA")  # e.g., add lang='pt', exclude_retweets=True

    # Collect with pagination until no pages or remaining quota
    print(f"[INFO] Remaining monthly quota (local): {remaining}")
    df, meta = scraper.search_tweets(
        query=query,
        max_results=100,
        paginate=True,
        total_limit=remaining,   # stop at remaining quota
        start_time=start_7d,
        end_time=now,
        since_id=None
    )

    print(f"\nCollected: {meta['collected']} tweets from {meta['pages']} pages")
    if meta['counts_endpoint'].get("supported"):
        print(f"Counts endpoint estimate (7 days): {meta['counts_endpoint']['total_estimated']}")

    # Save/append and update usage
    out_csv = scraper.output_dir / "copasa_tweets.csv"
    newly_added = scraper.save_or_append_csv(df, out_csv)
    scraper.add_usage(newly_added, monthly_cap=monthly_cap)
    print(f"[INFO] Usage updated. Newly added: {newly_added}")

if __name__ == '__main__':
    main()
