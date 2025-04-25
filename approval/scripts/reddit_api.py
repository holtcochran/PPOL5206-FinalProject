import sys

# Apply only in notebook-like environments
if 'ipykernel' in sys.modules:
    import nest_asyncio
    nest_asyncio.apply()

import asyncpraw
import pandas as pd
import datetime
import argparse
import os
import time
import asyncio
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Rate limiting constants
MAX_REQUESTS_PER_MINUTE = 60  # Reddit free tier limit is ~60 requests per minute
REQUEST_INTERVAL = 60 / MAX_REQUESTS_PER_MINUTE  # Time between requests in seconds


class AsyncRateLimiter:
    """Simple rate limiter to track and limit API requests"""

    def __init__(self, max_requests_per_minute=MAX_REQUESTS_PER_MINUTE):
        self.request_times = []
        self.interval = 60 / max_requests_per_minute

    async def wait_if_needed(self):
        """Wait if we've made too many requests recently"""
        current_time = time.time()

        # Remove timestamps older than 1 minute
        self.request_times = [t for t in self.request_times if current_time - t < 60]

        # If we've made too many requests in the last minute, wait
        if len(self.request_times) >= MAX_REQUESTS_PER_MINUTE:
            oldest_request = min(self.request_times)
            sleep_time = 60 - (current_time - oldest_request)
            if sleep_time > 0:
                print(f"Rate limit approached. Waiting {sleep_time:.2f} seconds...")
                await asyncio.sleep(sleep_time)

        # Add small delay between requests regardless
        elif len(self.request_times) > 0:
            last_request = max(self.request_times)
            time_since_last = current_time - last_request
            if time_since_last < self.interval:
                await asyncio.sleep(self.interval - time_since_last)

        # Record this request
        self.request_times.append(time.time())


async def authenticate_reddit():
    """Authenticate to Reddit API using credentials"""
    try:
        reddit = asyncpraw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'Comment Analyzer Script v1.0')
        )
        return reddit
    except Exception as e:
        print(f"Authentication Error: {e}")
        return None


async def search_posts(reddit, subreddits, search_query, post_limit=100000000, time_filter=None,
                 start_date=None, end_date=None, rate_limiter=None):
    """
    Search for posts in given subreddits containing the search query

    Parameters:
    - start_date: datetime object for the earliest post date to include
    - end_date: datetime object for the latest post date to include
    - time_filter: Reddit's time filter (hour, day, week, month, year, all)
    """
    all_posts = []

    # Convert single subreddit to list for consistent handling
    if isinstance(subreddits, str):
        subreddits = [subreddits]

    # Convert dates to timestamps for comparison
    start_timestamp = start_date.timestamp() if start_date else None
    end_timestamp = end_date.timestamp() if end_date else datetime.datetime.now().timestamp()

    for subreddit_name in subreddits:
        try:
            if rate_limiter:
                await rate_limiter.wait_if_needed()

            subreddit = await reddit.subreddit(subreddit_name)

            # Define search parameters
            search_params = {"query": search_query, "limit": None}  # No limit initially to allow date filtering
            if time_filter:
                search_params["time_filter"] = time_filter

            post_count = 0
            filtered_count = 0

            # If using a custom date range, we might need to fetch more posts to get enough in our date range
            fetch_limit = post_limit * 3 if start_date else post_limit

            async for post in subreddit.search(**search_params):
                if rate_limiter and post_count > 0 and post_count % 10 == 0:
                    await rate_limiter.wait_if_needed()

                post_count += 1

                # Check if post is within our date range
                post_time = post.created_utc
                if start_timestamp and post_time < start_timestamp:
                    filtered_count += 1
                    continue

                if post_time > end_timestamp:
                    filtered_count += 1
                    continue

                post_data = {
                    'subreddit': subreddit_name,
                    'post_id': post.id,
                    'title': post.title,
                    'author': str(post.author),
                    'created_utc': datetime.datetime.fromtimestamp(post.created_utc),
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'url': post.url,
                    'permalink': f"https://www.reddit.com{post.permalink}",
                    'is_self': post.is_self,
                    'selftext': post.selftext if post.is_self else "",
                }
                all_posts.append(post_data)

                print(f"\rCollected {len(all_posts)} posts (filtered out {filtered_count} based on date)", end="")

                # Stop if we've reached our limit
                if len(all_posts) >= post_limit:
                    break

                # Stop if we've fetched too many posts without finding enough in our date range
                if post_count >= fetch_limit:
                    break

        except Exception as e:
            print(f"\nError searching in r/{subreddit_name}: {e}")

    print()  # New line after progress indicator
    return all_posts


async def get_comments(reddit, posts, comment_limit=None, rate_limiter=None):
    """Get comments for each post"""
    all_comments = []
    total_posts = len(posts)

    for i, post in enumerate(posts, 1):
        try:
            if rate_limiter:
                await rate_limiter.wait_if_needed()

            print(f"\rProcessing comments for post {i}/{total_posts}: {post['title'][:50]}...", end="")

            # Get the submission by ID
            submission = await reddit.submission(id=post['post_id'])
            
            # Load the submission
            await submission.load()
            
            # Check if the post has comments before proceeding
            if submission.num_comments == 0:
                print(f"\rPost {post['post_id']} has no comments, skipping.", end="")
                continue
            
            # Process comments
            comment_count = 0
            comment_list = []
            
            try:
                # Safely access the comments
                if hasattr(submission, 'comments'):
                    if callable(submission.comments):
                        # If it's a method, call it
                        comment_forest = await submission.comments()
                    else:
                        # If it's an attribute
                        comment_forest = submission.comments
                    
                    # Process the comment forest
                    if hasattr(comment_forest, 'replace_more'):
                        await comment_forest.replace_more(limit=0)
                    
                    if hasattr(comment_forest, 'list'):
                        # Get flattened list of comments if possible
                        comment_list = await comment_forest.list()
                    elif hasattr(comment_forest, '__iter__'):
                        # Otherwise try direct iteration
                        comment_list = list(comment_forest)
                        
                        # Add child comments through breadth-first traversal
                        comment_index = 0
                        while comment_index < len(comment_list):
                            current = comment_list[comment_index]
                            if hasattr(current, 'replies') and hasattr(current.replies, '__iter__'):
                                try:
                                    comment_list.extend(current.replies)
                                except TypeError:
                                    # Skip if replies not iterable
                                    pass
                            comment_index += 1
            
                # Process each comment we found
                for comment in comment_list:
                    # Skip MoreComments objects
                    if isinstance(comment, asyncpraw.models.MoreComments):
                        continue
                    
                    # Check if we've reached the comment limit
                    if comment_limit is not None and comment_count >= comment_limit:
                        break
                    
                    try:
                        # Rate limiting check
                        if rate_limiter and comment_count > 0 and comment_count % 20 == 0:
                            await rate_limiter.wait_if_needed()
                        
                        # Build comment data
                        comment_data = {
                            'post_id': post['post_id'],
                            'comment_id': comment.id,
                            'author': str(comment.author),
                            'created_utc': datetime.datetime.fromtimestamp(comment.created_utc),
                            'score': comment.score,
                            'body': comment.body,
                            'permalink': f"https://www.reddit.com{comment.permalink}",
                            'parent_id': comment.parent_id,
                            'is_root': comment.parent_id.startswith('t3_'),
                            'depth': getattr(comment, 'depth', 0)
                        }
                        all_comments.append(comment_data)
                        comment_count += 1
                    
                    except Exception as e:
                        print(f"\nError processing individual comment: {e}")
                        continue
                
            except Exception as e:
                print(f"\nError accessing comments for post {post['post_id']}: {e}")
        
        except Exception as e:
            print(f"\nError getting comments for post {post['post_id']}: {e}")

    print()  # New line after progress indicator
    return all_comments


def save_data(query, posts, comments, output_dir="reddit_data"):
    """Save collected data to CSV files"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate timestamp for filenames
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Initialize result variables
    posts_file = None
    comments_file = None

    # Save posts
    if posts:
        posts_df = pd.DataFrame(posts)
        posts_file = f"{output_dir}/posts/{query}_posts_{timestamp}.csv"
        posts_df.to_csv(posts_file, index=False)
        print(f"Saved {len(posts)} posts to {posts_file}")

    # Save comments
    if comments:
        comments_df = pd.DataFrame(comments)
        comments_file = f"{output_dir}/comments/{query}_comments_{timestamp}.csv"
        comments_df.to_csv(comments_file, index=False)
        print(f"Saved {len(comments)} comments to {comments_file}")

    return posts_file, comments_file


def date_type(date_str):
    """Convert date string to datetime object for argparse"""
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


async def run_async_main(args):
    # Set up rate limiter
    rate_limiter = None if args.disable_rate_limit else AsyncRateLimiter(args.requests_per_minute)

    # Authenticate to Reddit API
    print("Authenticating to Reddit API...")
    reddit = await authenticate_reddit()
    if not reddit:
        return

    # Date range info for user feedback
    date_info = ""
    if args.start_date:
        date_info = f" from {args.start_date.strftime('%Y-%m-%d')}"
        if args.end_date:
            date_info += f" to {args.end_date.strftime('%Y-%m-%d')}"
        else:
            date_info += " to now"
    elif args.time_filter:
        date_info = f" (time filter: {args.time_filter})"

    print(f"Searching for '{args.query}' in r/{', r/'.join(args.subreddits)}{date_info}...")
    print(
        f"Rate limiting: {'Disabled' if args.disable_rate_limit else f'Max {args.requests_per_minute} requests/minute'}"
    )

    # Collect posts
    posts = await search_posts(
        reddit,
        args.subreddits,
        args.query,
        post_limit=args.post_limit,
        time_filter=args.time_filter,
        start_date=args.start_date,
        end_date=args.end_date,
        rate_limiter=rate_limiter
    )

    if not posts:
        print(f"No posts found with query '{args.query}'")
        return

    print(f"Found {len(posts)} posts. Collecting comments...")

    # Collect comments
    comments = await get_comments(
        reddit,
        posts,
        comment_limit=args.comment_limit,
        rate_limiter=rate_limiter
    )

    # Save data to CSV files
    posts_file, comments_file = save_data(args.query, posts, comments, output_dir=args.output_dir)

    # Optional: Load and analyze comments
    if comments_file:
        try:
            print(f"\nTop 5 most upvoted comments:")
            comments_df = pd.read_csv(comments_file)
            top_comments = comments_df.sort_values('score', ascending=False).head(5)
            for i, (_, comment) in enumerate(top_comments.iterrows(), 1):
                print(f"{i}. Score: {comment['score']} - {comment['body'][:100]}...")
        except Exception as e:
            print(f"Error analyzing comments: {e}")
            
    # Close the Reddit session
    try:
        await reddit.close()
    except Exception as e:  
        print(f"Error closing Reddit session: {e}")


def main():
    parser = argparse.ArgumentParser(description="Gather Reddit comments and reactions on specific topics")
    parser.add_argument("--subreddits", nargs="+", required=True, help="Subreddit(s) to search (space-separated)")
    parser.add_argument("--query", type=str, required=True, help="Search query/topic to look for")
    parser.add_argument("--post-limit", type=int, default=10000000000, help="Maximum number of posts to retrieve (default: 1M)")
    parser.add_argument("--comment-limit", type=int, default=None, help="Maximum comments per post (default: all)")

    # Date filtering options
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--time-filter", type=str, default=None,
                            choices=["hour", "day", "week", "month", "year", "all"],
                            help="Predefined time filter for posts")
    date_group.add_argument("--start-date", type=date_type, help="Start date for posts (format: YYYY-MM-DD)")

    parser.add_argument("--end-date", type=date_type,
                        help="End date for posts (format: YYYY-MM-DD, defaults to current date)")

    parser.add_argument("--output-dir", type=str, default="reddit_data", help="Output directory for data files")
    parser.add_argument("--disable-rate-limit", action="store_true", help="Disable rate limiting (not recommended)")
    parser.add_argument("--requests-per-minute", type=int, default=MAX_REQUESTS_PER_MINUTE,
                        help=f"Maximum API requests per minute (default: {MAX_REQUESTS_PER_MINUTE})")

    args = parser.parse_args()
    
    try:
        # If no event loop is running, use asyncio.run
        # Run the async main function
        asyncio.run(run_async_main(args))
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            # Fall back to using the current loop (for notebook-like environments)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(run_async_main(args))
        else:
            raise


if __name__ == "__main__":
    # python reddit_api.py --subreddits democrats Republican AskThe_Donald Conservative AskALiberal Liberal --query "usaid" --start-date 2025-01-20 --end-date 2025-04-16
    # AskThe_Donald Conservative
    # AskALiberal Liberal
    main()