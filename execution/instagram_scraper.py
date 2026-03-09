#!/usr/bin/env python3
"""
Instagram Scraper — Apify API integration
Fetches Instagram profile data, reels, and discovers competitors/influencers.

Actors used:
- apify/instagram-reel-scraper — Dedicated reel scraper with native recency filter
- apify/instagram-profile-scraper — Profile basics (bio, followers, etc.)
- apify/google-search-scraper — Competitor discovery via Google (site:instagram.com search)
- apify/instagram-hashtag-scraper — Hashtag-based influencer discovery
"""
import os
import time
import logging
import requests

logger = logging.getLogger(__name__)

APIFY_BASE = "https://api.apify.com/v2"


def _get_api_key():
    key = os.getenv("APIFY_API_KEY", "")
    if not key:
        raise ValueError("APIFY_API_KEY not set in environment")
    return key


def _run_actor(actor_id, input_data, timeout_secs=300):
    """Run an Apify actor synchronously and return the dataset items."""
    api_key = _get_api_key()
    # Apify API uses ~ instead of / in actor IDs
    safe_id = actor_id.replace('/', '~')
    url = f"{APIFY_BASE}/acts/{safe_id}/run-sync-get-dataset-items"
    params = {"token": api_key, "timeout": timeout_secs}
    headers = {"Content-Type": "application/json"}

    logger.info(f"Running Apify actor: {actor_id}")
    resp = requests.post(url, json=input_data, params=params, headers=headers, timeout=timeout_secs + 30)

    if resp.status_code not in (200, 201):
        logger.error(f"Apify actor {actor_id} failed: {resp.status_code} — {resp.text[:500]}")
        return []

    try:
        items = resp.json()
        logger.info(f"Apify actor {actor_id} returned {len(items)} items")
        return items
    except Exception as e:
        logger.error(f"Failed to parse Apify response: {e}")
        return []


def _run_actor_async(actor_id, input_data, timeout_secs=300):
    """Run an Apify actor asynchronously — start run, poll for completion, fetch dataset."""
    api_key = _get_api_key()

    # Start the run — Apify API uses ~ instead of / in actor IDs
    safe_id = actor_id.replace('/', '~')
    start_url = f"{APIFY_BASE}/acts/{safe_id}/runs"
    params = {"token": api_key}
    headers = {"Content-Type": "application/json"}

    logger.info(f"Starting async Apify actor: {actor_id}")
    resp = requests.post(start_url, json=input_data, params=params, headers=headers, timeout=30)
    if resp.status_code not in (200, 201):
        logger.error(f"Apify start failed: {resp.status_code} — {resp.text[:500]}")
        return []

    run_data = resp.json().get("data", {})
    run_id = run_data.get("id")
    if not run_id:
        logger.error("No run ID returned from Apify")
        return []

    # Poll for completion
    poll_url = f"{APIFY_BASE}/actor-runs/{run_id}"
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        time.sleep(5)
        poll_resp = requests.get(poll_url, params={"token": api_key}, timeout=15)
        if poll_resp.status_code == 200:
            status = poll_resp.json().get("data", {}).get("status")
            logger.info(f"Actor run {run_id} status: {status}")
            if status == "SUCCEEDED":
                break
            elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                logger.error(f"Actor run {run_id} ended with status: {status}")
                return []
    else:
        logger.error(f"Actor run {run_id} timed out after {timeout_secs}s")
        return []

    # Fetch dataset items
    dataset_id = poll_resp.json().get("data", {}).get("defaultDatasetId")
    if not dataset_id:
        logger.error("No dataset ID from completed run")
        return []

    items_url = f"{APIFY_BASE}/datasets/{dataset_id}/items"
    
    # Retry fetching the dataset to prevent 502/503 errors when Apify is under load
    for attempt in range(4):
        try:
            items_resp = requests.get(items_url, params={"token": api_key, "format": "json"}, timeout=45)
            if items_resp.status_code == 200:
                items = items_resp.json()
                logger.info(f"Fetched {len(items)} items from dataset {dataset_id}")
                return items
            elif items_resp.status_code in (500, 502, 503, 504):
                wait_time = 2 ** attempt
                logger.warning(f"Apify dataset fetch failed with {items_resp.status_code}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch dataset: {items_resp.status_code} - {items_resp.text[:200]}")
                return []
        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt
            logger.warning(f"Network error fetching dataset: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
            
    logger.error("Failed to fetch dataset after multiple retries.")
    return []


# ─────────────────────────────────────────────
# Public Functions
# ─────────────────────────────────────────────

def scrape_instagram_profile(username):
    """
    Scrape an Instagram profile's basic info.
    Returns: {username, full_name, bio, followers, following, posts_count, profile_pic_url, is_verified, ...}
    """
    input_data = {
        "usernames": [username]
    }
    items = _run_actor("apify/instagram-profile-scraper", input_data, timeout_secs=120)
    if items:
        profile = items[0]
        return {
            "username": profile.get("username", username),
            "full_name": profile.get("fullName", ""),
            "bio": profile.get("biography", ""),
            "followers": profile.get("followersCount", 0),
            "following": profile.get("followsCount", 0),
            "posts_count": profile.get("postsCount", 0),
            "profile_pic_url": profile.get("profilePicUrl", ""),
            "is_verified": profile.get("verified", False),
            "is_business": profile.get("isBusinessAccount", False),
            "category": profile.get("businessCategoryName", ""),
            "external_url": profile.get("externalUrl", ""),
            "engagement_rate": _calc_engagement_rate(profile),
            "raw": profile
        }
    return None


def screenshot_instagram_profile(username):
    """
    Take a screenshot of an Instagram profile page using Apify's screenshot actor.
    Returns a publicly accessible image URL, or None on failure.
    The returned URL is a temporary Apify download link (expires in ~5 min),
    so it should be downloaded/used immediately.
    """
    import time as _time
    api_key = _get_api_key()
    profile_url = f"https://www.instagram.com/{username}/"
    
    logger.info(f"Taking screenshot of Instagram profile: {profile_url}")
    
    # Start the actor run asynchronously
    # Switched to onescales/website-screenshot-pro to handle Instagram login walls via proxies,
    # and to natively output PNGs instead of WebP.
    actor_id = "onescales~website-screenshot-pro"
    start_url = f"{APIFY_BASE}/acts/{actor_id}/runs"
    
    input_data = {
        "urls": [profile_url],
        "widths": ["1920"],
        "captureFullPage": True,
        "proxyConfiguration": {
            "useApifyProxy": True
        },
        "delayBeforeScreenshot": 3000,
        "formats": ["png"]
    }
    
    try:
        resp = requests.post(
            start_url, json=input_data,
            params={"token": api_key},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if resp.status_code not in (200, 201):
            logger.error(f"Screenshot actor start failed: {resp.status_code} — {resp.text[:300]}")
            return None
        
        run_data = resp.json().get("data", {})
        run_id = run_data.get("id")
        dataset_id = run_data.get("defaultDatasetId")
        
        if not run_id:
            logger.error("No run ID returned from screenshot actor")
            return None
        
        logger.info(f"Screenshot run started: {run_id}")
        
        # Poll for completion (max ~3 minutes)
        for i in range(18):
            _time.sleep(10)
            poll_resp = requests.get(
                f"{APIFY_BASE}/actor-runs/{run_id}",
                params={"token": api_key}, timeout=15
            )
            status = poll_resp.json().get("data", {}).get("status", "UNKNOWN")
            logger.info(f"Screenshot poll {i+1}: {status}")
            if status in ("SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"):
                break
        
        if status != "SUCCEEDED":
            logger.error(f"Screenshot actor ended with: {status}")
            return None
        
        # Fetch dataset items
        ds_url = f"{APIFY_BASE}/datasets/{dataset_id}/items"
        ds_resp = requests.get(ds_url, params={"token": api_key, "format": "json"}, timeout=30)
        items = ds_resp.json()
        
        if not items:
            logger.error("Screenshot actor returned no items")
            return None
        
        item = items[0]
        # onescales actor exports the image url inside the 'width_1920' field based on specified widths
        download_url = item.get("width_1920")
        if not download_url:
            logger.error(f"No image URL in screenshot response. Keys: {list(item.keys())}")
            return None
            
        # (The temp URL expires in 5 min, so we must convert now)
        img_resp = requests.get(download_url, timeout=30)
        if img_resp.status_code != 200:
            logger.error(f"Failed to download screenshot image: {img_resp.status_code}")
            return None
        
        import base64
        content_type = img_resp.headers.get("Content-Type", "image/webp")
        b64 = base64.b64encode(img_resp.content).decode("utf-8")
        data_uri = f"data:{content_type};base64,{b64}"
        
        logger.info(f"Screenshot captured for @{username}: {len(img_resp.content)} bytes")
        return data_uri
        
    except Exception as e:
        logger.error(f"Screenshot capture failed for @{username}: {e}")
        return None


def scrape_instagram_reels(username, max_reels=20, profile_data=None, recency_days=180):
    """
    Extract recent reels from an Instagram profile.
    If profile_data (from scrape_instagram_profile) is provided, extracts from there instantly.
    Otherwise, runs a profile scrape to get them.
    Returns list of: {id, url, video_url, caption, views, likes, comments, timestamp, hashtags, ...}
    """
    if not profile_data:
        profile_data = scrape_instagram_profile(username)
        if not profile_data:
            return []

    # profile_data["raw"] contains the original Apify response
    raw = profile_data.get("raw", profile_data)
    posts = raw.get("latestPosts", []) or raw.get("recentPosts", []) or []
    
    # Debug: log all post types if we have any
    if posts:
        types_found = set(p.get("type", "unknown") for p in posts)
        logger.info(f"@{username}: {len(posts)} latestPosts, types: {types_found}")
    else:
        logger.warning(f"@{username}: no latestPosts found in profile data. Raw keys: {list(raw.keys())[:10]}")
        return []
    
    # Filter for videos/reels — be very permissive with type matching
    video_types = {"Video", "VideoClip", "Reel", "GraphVideo", "video", "Clip"}
    videos = [p for p in posts if 
              p.get("type") in video_types or 
              p.get("videoUrl") or 
              p.get("isVideo") or
              p.get("videoViewCount")]

    logger.info(f"@{username}: {len(videos)} video/reel posts out of {len(posts)} total")

    # ── Recency filter: only keep reels within the chosen window ──
    from datetime import datetime, timedelta
    if recency_days > 0:
        cutoff = datetime.utcnow() - timedelta(days=recency_days)
        recent_videos = []
        for v in videos:
            ts = v.get("timestamp", "")
            if ts:
                try:
                    post_date = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
                    if post_date < cutoff:
                        logger.info(f"@{username}: skipping old reel from {post_date.strftime('%Y-%m-%d')}")
                        continue
                except (ValueError, TypeError):
                    pass  # If we can't parse the date, include it
            recent_videos.append(v)
        
        if len(recent_videos) < len(videos):
            logger.info(f"@{username}: {len(videos)} → {len(recent_videos)} after {recency_days}-day recency filter")
        videos = recent_videos
    else:
        logger.info(f"@{username}: recency filter disabled (All Time)")

    reels = []
    for item in videos[:max_reels]:
        reel = {
            "id": str(item.get("id", "")),
            "url": item.get("url", "") or f"https://www.instagram.com/p/{item.get('shortCode', '')}/",
            "video_url": item.get("videoUrl", ""),
            "caption": item.get("caption", ""),
            "views": item.get("videoViewCount", 0) or item.get("playCount", 0) or 0,
            "likes": item.get("likesCount", 0) or 0,
            "comments": item.get("commentsCount", 0) or 0,
            "timestamp": item.get("timestamp", ""),
            "hashtags": item.get("hashtags", []),
            "music": item.get("musicInfo", {}),
            "thumbnail_url": item.get("displayUrl", ""),
            "owner_username": item.get("ownerUsername", username),
            "raw": item
        }
        # Include all video reels (even without videoUrl — they just won't be transcribed)
        total_engagement = reel["likes"] + reel["comments"]
        reel["total_engagement"] = total_engagement
        reels.append(reel)

    # Sort by views descending (outliers first)
    reels.sort(key=lambda x: x.get("views", 0), reverse=True)
    return reels


def scrape_competitor_reels_batch(usernames, max_reels_per_profile=20, recency_days=180):
    """
    Use apify/instagram-reel-scraper to fetch reels for multiple competitors in one batch call.
    This actor has native recency filtering (onlyPostsNewerThan) and returns rich reel data
    including views, likes, shares, captions, transcripts.
    
    Returns: dict mapping username -> list of reels in our standard format.
    """
    if not usernames:
        return {}
    
    # Convert recency_days to the format the actor expects
    recency_str = f"{recency_days} days" if recency_days > 0 else None
    
    input_data = {
        "username": usernames,
        "resultsLimit": max_reels_per_profile,
    }
    if recency_str:
        input_data["onlyPostsNewerThan"] = recency_str
    
    logger.info(f"Batch scraping reels for {len(usernames)} competitors via instagram-reel-scraper "
                f"(max {max_reels_per_profile}/profile, recency: {recency_str or 'all time'})...")
    
    items = _run_actor_async("apify/instagram-reel-scraper", input_data, timeout_secs=300)
    
    if not items:
        logger.warning("Reel scraper returned no items")
        return {}
    
    logger.info(f"Reel scraper returned {len(items)} total reels across all competitors")
    
    # Group by owner username and convert to our standard reel format
    reels_by_user = {}
    for item in items:
        owner = item.get("ownerUsername", "")
        if not owner:
            continue
        
        reel = {
            "id": str(item.get("id", "")),
            "url": item.get("url", "") or f"https://www.instagram.com/p/{item.get('shortCode', '')}/",
            "video_url": item.get("videoUrl", ""),
            "caption": item.get("caption", ""),
            "views": item.get("videoPlayCount", 0) or item.get("videoViewCount", 0) or 0,
            "likes": item.get("likesCount", 0) or 0,
            "comments": item.get("commentsCount", 0) or 0,
            "timestamp": item.get("timestamp", ""),
            "hashtags": item.get("hashtags", []),
            "music": item.get("musicInfo", {}),
            "thumbnail_url": item.get("displayUrl", ""),
            "owner_username": owner,
            "total_engagement": (item.get("likesCount", 0) or 0) + (item.get("commentsCount", 0) or 0),
            "raw": item
        }
        
        if owner not in reels_by_user:
            reels_by_user[owner] = []
        reels_by_user[owner].append(reel)
    
    # Sort each competitor's reels by views descending
    for user in reels_by_user:
        reels_by_user[user].sort(key=lambda x: x.get("views", 0), reverse=True)
    
    logger.info(f"Reels grouped for {len(reels_by_user)} competitors: "
                f"{', '.join(f'@{u}={len(r)}' for u, r in reels_by_user.items())}")
    
    return reels_by_user


def discover_competitors(niche_keyword, location="", min_followers=50000, max_followers=1000000, limit=10):
    """
    Discover top-tier competitors using Google Search + Instagram Profile Scraper.
    
    Step 1: Google search for 'site:instagram.com {niche} {location}' 
            and broader variants to find high-follower accounts.
    Step 2: Extract usernames from Google URLs
    Step 3: Batch scrape profiles
    Step 4: Filter strictly by min_followers and max_followers
            (We want established creators, not tiny accounts where data is noisy)
    """
    import re
    
    # We want broader searches to find the big players.
    # E.g., if location is "Delhi", searching "India" finds the massive accounts we want to emulate.
    queries = [
        f"site:instagram.com {niche_keyword} {location}".strip(),
        f"site:instagram.com {niche_keyword} india" if "india" not in location.lower() else f"site:instagram.com top {niche_keyword}",
        f"site:instagram.com \"creator\" OR \"public figure\" {niche_keyword}",
        f"site:instagram.com best {niche_keyword} influencer {location}".strip()
    ]
    
    usernames = []
    
    for query in queries:
        logger.info(f"Step 1: Google search: '{query}'")
        google_results = _run_actor_async("apify/google-search-scraper", {
            "queries": query,
            "maxPagesPerQuery": 2,
            "resultsPerPage": 30
        }, timeout_secs=120)
        
        for page in google_results:
            for result in page.get("organicResults", []):
                url = result.get("url", "")
                match = re.search(r'instagram\.com/([a-zA-Z0-9_\.]+)', url)
                if match:
                    username = match.group(1)
                    skip = {"p", "reel", "reels", "explore", "stories", "accounts", "tags", "about", "directory"}
                    if username.lower() not in skip:
                        usernames.append(username)
                        
    # Deduplicate
    usernames = list(dict.fromkeys(usernames))
    
    if not usernames:
        logger.error("No competitor usernames found from Google search!")
        return []
        
    # We fetch a larger batch because we'll filter many out based on follower counts
    fetch_limit = min(limit * 4, 40)
    target_usernames = usernames[:fetch_limit]
    
    logger.info(f"Step 2: Extracted {len(usernames)} usernames. Scraping top {len(target_usernames)} to check follower counts...")
    
    profiles = _run_actor_async("apify/instagram-profile-scraper", {
        "usernames": target_usernames
    }, timeout_secs=180)
    
    competitors = []
    for item in profiles:
        username = item.get("username")
        if not username or item.get("error"):
            continue
        
        followers = item.get("followersCount", 0) or 0
        
        # Step 4: STRICT FOLLOWER FILTERING
        # We only want established accounts where outliers are statistically significant.
        if followers < min_followers:
            logger.info(f"Skipping @{username} — too small ({followers:,} < {min_followers:,})")
            continue
        if followers > max_followers:
            logger.info(f"Skipping @{username} — too big ({followers:,} > {max_followers:,})")
            continue
            
        competitors.append({
            "username": username,
            "full_name": item.get("fullName", ""),
            "followers": followers,
            "profile_pic_url": item.get("profilePicUrl", ""),
            "engagement_rate": _calc_engagement_rate(item),
            "is_verified": item.get("verified", False),
            "category": item.get("businessCategoryName", ""),
            "raw": item  # Full profile with latestPosts for reel extraction
        })
    
    # Sort by followers descending
    competitors.sort(key=lambda x: x["followers"], reverse=True)
    
    top = competitors[:limit]
    logger.info(f"Discovered {len(top)} QUALIFIED competitors ({min_followers}-{max_followers} followers): {[(c['username'], c['followers']) for c in top]}")
    
    # If we didn't find enough qualified competitors, just take the biggest ones we found, period.
    if not top and len(profiles) > 0:
        logger.warning(f"No competitors matched the {min_followers}-{max_followers} filter. Returning biggest available.")
        fallback = []
        for item in profiles:
            if not item.get("username") or item.get("error"):
                continue
            fallback.append({
                "username": item["username"],
                "full_name": item.get("fullName", ""),
                "followers": item.get("followersCount", 0) or 0,
                "profile_pic_url": item.get("profilePicUrl", ""),
                "engagement_rate": _calc_engagement_rate(item),
                "is_verified": item.get("verified", False),
                "category": item.get("businessCategoryName", ""),
                "raw": item
            })
        fallback.sort(key=lambda x: x["followers"], reverse=True)
        return fallback[:limit]
        
    return top


def find_influencers_by_niche(niche_keyword, location="", min_followers=10000, max_followers=100000, limit=50):
    """
    Discover influencers using a multi-source strategy for high volume:
    
    Source 1: Instagram Search API — username search for niche + location
    Source 2: Hashtag-based discovery via apify/instagram-hashtag-scraper —
              scrapes posts from niche hashtags, extracts unique account usernames
    Source 3: Google fallback if above return nothing
    
    All sources are deduplicated, then batch-profiled and filtered by follower range.
    Default limit is 50 to support bulk outreach workflows.
    """
    import re
    
    search_query = f"{niche_keyword} {location}".strip()
    
    logger.info(f"Influencer Discovery: '{search_query}' ({min_followers:,}-{max_followers:,} followers, limit={limit})")
    
    all_usernames = []  # Collect from all sources, deduplicate later
    
    # ── SOURCE 1: Instagram Search API ──
    logger.info("Source 1: Instagram user search...")
    search_results = _run_actor_async("apify/instagram-search-scraper", {
        "search": search_query,
        "searchType": "user",
        "resultsLimit": min(limit * 5, 200)
    }, timeout_secs=120)
    
    search_candidates = []
    for item in (search_results or []):
        username = item.get("username") or item.get("name", "")
        followers = item.get("followersCount", 0) or item.get("followers", 0) or 0
        
        if not username:
            continue
        
        skip = {"p", "reel", "reels", "explore", "stories", "accounts", "tags", "about", "directory"}
        if username.lower() in skip:
            continue
        
        # Pre-filter by follower range (using search data — rough but saves API calls)
        if followers < min_followers or followers > max_followers:
            continue
        
        search_candidates.append(username)
    
    logger.info(f"Instagram search: {len(search_results or [])} results → {len(search_candidates)} in follower range")
    all_usernames.extend(search_candidates)
    
    # ── SOURCE 2: Hashtag-based Discovery ──
    # Generate niche hashtags from the keyword + location
    niche_clean = re.sub(r'[^a-zA-Z0-9]', '', niche_keyword.lower())
    loc_clean = re.sub(r'[^a-zA-Z0-9]', '', location.lower()) if location else ""
    
    hashtags = [niche_clean]
    if loc_clean:
        hashtags.append(f"{niche_clean}{loc_clean}")
        hashtags.append(loc_clean + niche_clean)
        
    # Add targeted variants for better brand/business discovery
    commercial_suffixes = ["startup", "business", "store", "shop", "brand", "company", "india"]
    creator_suffixes = ["tips", "expert", "coach", "community", "creator"]
    
    all_suffixes = commercial_suffixes + creator_suffixes
    for suffix in all_suffixes:
        hashtags.append(f"{niche_clean}{suffix}")
        if loc_clean:
             # e.g., foodbrandindianstartup, foodbrandmumbaistore
             hashtags.append(f"{niche_clean}{loc_clean}{suffix}")
    
    # Deduplicate hashtags
    hashtags = list(dict.fromkeys(hashtags))
    
    logger.info(f"Source 2: Hashtag discovery with {hashtags}...")
    hashtag_results = _run_actor_async("apify/instagram-hashtag-scraper", {
        "hashtags": hashtags,
        "resultsLimit": min(limit * 50, 1000),
        "resultsType": "posts"
    }, timeout_secs=180)
    
    hashtag_usernames = []
    for post in (hashtag_results or []):
        owner = post.get("ownerUsername", "") or post.get("owner_username", "")
        if not owner:
            # Try nested owner object
            owner_obj = post.get("owner", {})
            owner = owner_obj.get("username", "") if owner_obj else ""
        if owner and owner.lower() not in {"p", "reel", "reels", "explore"}:
            hashtag_usernames.append(owner)
    
    # Deduplicate hashtag usernames
    hashtag_usernames = list(dict.fromkeys(hashtag_usernames))
    logger.info(f"Hashtag discovery: {len(hashtag_results or [])} posts → {len(hashtag_usernames)} unique accounts")
    all_usernames.extend(hashtag_usernames)
    
    # ── DEDUPLICATE all sources ──
    all_usernames = list(dict.fromkeys(all_usernames))
    logger.info(f"Total unique candidates from all sources: {len(all_usernames)}")
    
    if not all_usernames:
        logger.warning("No candidates found from any source, falling back to Google search")
        return _find_influencers_google_fallback(niche_keyword, location, min_followers, max_followers, limit)
    
    # ── BATCH PROFILE SCRAPE (Chunked Loop) ──
    influencers = []
    chunk_size = 30  # Number of profiles to verify per batch
    loc_lower = location.lower() if location else ""
    
    while len(all_usernames) > 0 and len(influencers) < limit:
        # Take the next chunk of usernames
        target_usernames = all_usernames[:chunk_size]
        all_usernames = all_usernames[chunk_size:]
        
        logger.info(f"Batch-scraping {len(target_usernames)} profiles for verification... (Found so far: {len(influencers)}/{limit})")
        
        profiles = _run_actor_async("apify/instagram-profile-scraper", {
            "usernames": target_usernames
        }, timeout_secs=240)
        
        for item in (profiles or []):
            username = item.get("username")
            if not username or item.get("error"):
                continue
            
            followers = item.get("followersCount", 0) or 0
            
            # Strict follower range check with actual profile data
            if followers < min_followers or followers > max_followers:
                continue
                
            # Strict Location Filtering
            if loc_lower:
                bio = (item.get("biography") or "").lower()
                full_name = (item.get("fullName") or "").lower()
                category = (item.get("businessCategoryName") or "").lower()
                uname_lower = username.lower()
                
                # Check strict string match
                matched = False
                search_text = f"{bio} {full_name} {category} {uname_lower}"
                if loc_lower in search_text:
                    matched = True
                
                # Broaden context for "India" specifically, to avoid skipping brands that just list their city
                if not matched and loc_lower == "india":
                    india_cities = ["mumbai", "delhi", "bangalore", "bengaluru", "chennai", "hyderabad", "pune", "kolkata", "ahmedabad", "noida", "gurgaon"]
                    for city in india_cities:
                        if city in search_text:
                            matched = True
                            break
                            
                # If the location keyword isn't found anywhere in the profile data, skip it
                if not matched:
                    continue
                
            influencers.append({
                "username": username,
                "full_name": item.get("fullName", ""),
                "followers": followers,
                "following": item.get("followsCount", 0) or 0,
                "bio": item.get("biography", ""),
                "profile_pic_url": item.get("profilePicUrl", ""),
                "engagement_rate": _calc_engagement_rate(item),
                "is_verified": item.get("verified", False),
                "category": item.get("businessCategoryName", ""),
                "external_url": item.get("externalUrl", "")
            })
            
    # Sort by engagement rate to surface the best prospects
    influencers.sort(key=lambda x: x["engagement_rate"], reverse=True)
    
    top = influencers[:limit]
    logger.info(f"Influencer Discovery COMPLETE: {len(top)} qualified matching influencers "
                f"({min_followers:,}-{max_followers:,} followers) returned.")
    
    return top


def _find_influencers_google_fallback(niche_keyword, location="", min_followers=10000, max_followers=100000, limit=20):
    """Fallback: use Google search if Instagram search returns nothing."""
    import re
    
    # Use multiple queries to cast a wider net
    queries = [
        f"site:instagram.com {niche_keyword} {location}".strip(),
        f"site:instagram.com \"creator\" OR \"public figure\" {niche_keyword} {location}".strip(),
        f"site:instagram.com best {niche_keyword} influencer {location}".strip(),
        f"site:instagram.com top {niche_keyword} blogger {location}".strip()
    ]
    
    usernames = []
    for query in queries:
        logger.info(f"Fallback: Google search '{query}'")
        google_results = _run_actor_async("apify/google-search-scraper", {
            "queries": query,
            "maxPagesPerQuery": 2,
            "resultsPerPage": 30
        }, timeout_secs=120)
        
        for page in google_results:
            for result in page.get("organicResults", []):
                url = result.get("url", "")
                match = re.search(r'instagram\.com/([a-zA-Z0-9_\.]+)', url)
                if match:
                    username = match.group(1)
                    skip = {"p", "reel", "reels", "explore", "stories", "accounts", "tags", "about", "directory"}
                    if username.lower() not in skip:
                        usernames.append(username)
    
    usernames = list(dict.fromkeys(usernames))
    if not usernames:
        return []
    
    target = usernames[:min(limit * 2, 30)]
    profiles = _run_actor_async("apify/instagram-profile-scraper", {
        "usernames": target
    }, timeout_secs=180)
    
    influencers = []
    for item in profiles:
        username = item.get("username")
        if not username or item.get("error"):
            continue
        followers = item.get("followersCount", 0) or 0
        if followers < min_followers or followers > max_followers:
            continue
        influencers.append({
            "username": username,
            "full_name": item.get("fullName", ""),
            "followers": followers,
            "following": item.get("followsCount", 0) or 0,
            "bio": item.get("biography", ""),
            "profile_pic_url": item.get("profilePicUrl", ""),
            "engagement_rate": _calc_engagement_rate(item),
            "is_verified": item.get("verified", False),
            "category": item.get("businessCategoryName", ""),
            "external_url": item.get("externalUrl", "")
        })
    
    influencers.sort(key=lambda x: x["engagement_rate"], reverse=True)
    return influencers[:limit]


def get_best_reels_from_competitor_list(competitors, limit=7, top_reels=15, recency_days=180):
    """
    Computes outlier reels using an already-discovered list of competitors.
    Uses apify/instagram-reel-scraper for reliable, recency-filtered reel data.
    """
    logger.info(f"=== REEL OUTLIER ENGINE: Processing {len(competitors)} pre-found competitors ===")
    
    # Batch-fetch reels for all competitors at once using the dedicated reel scraper
    usernames = [comp["username"] for comp in competitors[:limit]]
    reels_by_user = scrape_competitor_reels_batch(usernames, max_reels_per_profile=30, recency_days=recency_days)
    
    all_reels = []
    competitors_data = []
    
    for comp in competitors[:limit]:
        uname = comp["username"]
        followers = comp.get("followers", 0)
        reels = reels_by_user.get(uname, [])
        
        if not reels:
            logger.warning(f"@{uname}: no reels found")
            continue
        
        logger.info(f"@{uname}: {len(reels)} reels fetched")
        
        # Compute average engagement for this competitor's reels
        avg_eng = sum(r.get("likes", 0) + r.get("comments", 0) for r in reels) / max(len(reels), 1)
        
        comp_info = {
            "username": uname,
            "full_name": comp.get("full_name", ""),
            "followers": followers,
            "profile_pic_url": comp.get("profile_pic_url", ""),
            "engagement_rate": comp.get("engagement_rate", 0),
            "avg_engagement": avg_eng,
            "reels_count": len(reels)
        }
        competitors_data.append(comp_info)
        
        # Score every reel
        for reel in reels:
            reel_eng = reel.get("likes", 0) + reel.get("comments", 0)
            outlier_vs_self = reel_eng / max(avg_eng, 1)
            eng_follower_ratio = (reel_eng / max(followers, 1)) * 100
            outlier_score = outlier_vs_self * eng_follower_ratio
            
            reel["outlier_score"] = round(outlier_score, 2)
            reel["outlier_vs_self"] = round(outlier_vs_self, 2)
            reel["eng_follower_ratio"] = round(eng_follower_ratio, 2)
            reel["competitor"] = comp_info
            
            all_reels.append(reel)
    
    logger.info(f"Total reels collected: {len(all_reels)} from {len(competitors_data)} competitors")
    
    # Sort by outlier score descending
    all_reels.sort(key=lambda x: x.get("outlier_score", 0), reverse=True)
    
    top = all_reels[:top_reels]
    if top:
        logger.info(f"Top outlier: @{top[0].get('competitor', {}).get('username', '?')} — "
                     f"score={top[0]['outlier_score']}, views={top[0].get('views', 0)}")
                     
    return top


def get_top_competitors_best_reels(niche_keyword, location="", limit=7, top_reels=15, recency_days=180):
    """
    Complete Competitor Outlier Engine:
    1. Discover top competitors via Google Search + Instagram Profile Scraper
    2. Batch-fetch their reels via apify/instagram-reel-scraper (with recency filter)
    3. Compute outlier score for each reel
    4. Return the top N outlier reels across all competitors
    """
    logger.info(f"=== COMPETITOR OUTLIER ENGINE START: {niche_keyword} in {location} ===")
    
    competitors = discover_competitors(niche_keyword, location, limit=limit)
    if not competitors:
        logger.warning(f"No competitors found for {niche_keyword} {location}")
        return []

    # Use the dedicated reel scraper in a single batch call
    return get_best_reels_from_competitor_list(competitors, limit=limit, top_reels=top_reels, recency_days=recency_days)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _calc_engagement_rate(profile):
    """Calculate engagement rate from profile data."""
    followers = profile.get("followersCount", 0) or profile.get("followers", 0)
    if not followers or followers == 0:
        return 0

    # Try to get from recent posts if available
    recent_posts = profile.get("latestPosts", []) or profile.get("recentPosts", [])
    if recent_posts:
        total_eng = sum(
            (p.get("likesCount", 0) or 0) + (p.get("commentsCount", 0) or 0)
            for p in recent_posts[:12]
        )
        avg_eng = total_eng / min(len(recent_posts), 12)
        return round((avg_eng / followers) * 100, 2)

    return 0

def find_influencers_by_seed(seed_username, location="", min_followers=10000, max_followers=100000, limit=20):
    """
    Lookalike Crawler: 
    Finds influencers by recursively asking Instagram for "Related Accounts" 
    starting from a seed account.
    """
    logger.info(f"Influencer Discovery by Seed: '{seed_username}' ({min_followers}-{max_followers} followers, limit={limit})")
    
    influencers = []
    seen_usernames = set()
    target_seeds = [seed_username.lower().replace("@", "")]
    loc_lower = location.lower() if location else ""
    
    # Track the original seed so we don't accidentally return it as a new discovery
    seen_usernames.add(target_seeds[0])
    
    # We will loop until we have enough qualified influencers OR we run out of seeds
    while len(target_seeds) > 0 and len(influencers) < limit:
        current_seed = target_seeds.pop(0)
        
        logger.info(f"Crawling related accounts for seed: @{current_seed}...")
        related_results = _run_actor_async("thenetaji/instagram-related-user-scraper", {
            "profileUrls": [f"https://www.instagram.com/{current_seed}/"],
            "maxItems": 100
        }, timeout_secs=120)
        
        new_candidates = []
        for res in (related_results or []):
            related_username = res.get("username")
            if related_username and related_username.lower() not in seen_usernames:
                new_candidates.append(related_username.lower())
                seen_usernames.add(related_username.lower())
                
        if not new_candidates:
            logger.info(f"No new candidates found for @{current_seed}. Moving to next.")
            continue
            
        logger.info(f"Found {len(new_candidates)} related accounts for @{current_seed}. Batch verifying...")
        
        # Batch verify the new candidates using profile-scraper for accurate followers & location
        chunk_size = 30
        for i in range(0, len(new_candidates), chunk_size):
            chunk = new_candidates[i:i+chunk_size]
            
            # If we've hit the limit while processing chunks of this seed's children, break out
            if len(influencers) >= limit:
                break
                
            profiles = _run_actor_async("apify/instagram-profile-scraper", {
                "usernames": chunk
            }, timeout_secs=240)
            
            for item in (profiles or []):
                username = item.get("username")
                if not username or item.get("error"):
                    continue
                
                followers = item.get("followersCount", 0) or 0
                
                if followers < min_followers or followers > max_followers:
                    continue
                    
                # Strict Location Filtering
                if loc_lower:
                    bio = (item.get("biography") or "").lower()
                    full_name = (item.get("fullName") or "").lower()
                    category = (item.get("businessCategoryName") or "").lower()
                    uname_lower = username.lower()
                    
                    if loc_lower not in bio and loc_lower not in full_name and loc_lower not in category and loc_lower not in uname_lower:
                        continue
                        
                # Passes all checks! 
                influencers.append({
                    "username": username,
                    "full_name": item.get("fullName", ""),
                    "followers": followers,
                    "following": item.get("followsCount", 0) or 0,
                    "bio": item.get("biography", ""),
                    "profile_pic_url": item.get("profilePicUrl", ""),
                    "engagement_rate": _calc_engagement_rate(item),
                    "is_verified": item.get("verified", False),
                    "category": item.get("businessCategoryName", ""),
                    "external_url": item.get("externalUrl", "")
                })
                
                # Add this newly qualified influencer to our seed queue to crawl *their* related accounts next if needed
                if username.lower() not in target_seeds:
                    target_seeds.append(username.lower())
                    
                # Stop adding if limit reached
                if len(influencers) >= limit:
                    break
    
    # Sort final results by engagement rate 
    influencers.sort(key=lambda x: x["engagement_rate"], reverse=True)
    top = influencers[:limit]
    
    logger.info(f"Seed Discovery COMPLETE: {len(top)} qualified matching influencers returned.")
    return top

def _get_serper_api_key():
    key = os.getenv("SERPER_API_KEY")
    if not key:
        raise ValueError("SERPER_API_KEY not set in environment")
    return key


def _parse_followers_from_snippet(snippet):
    """
    Instagram meta snippets usually follow this format: 
    '14K Followers, 1,200 Following, 300 Posts - See Instagram photos and videos from...'
    This function extracts that first number and converts 'K' or 'M' to an integer.
    """
    import re
    if not snippet:
        return 0
        
    match = re.search(r'([\d\.,]+)([kKmM]?)\s+Followers', snippet, re.IGNORECASE)
    if not match:
        return 0
        
    num_str = match.group(1).replace(',', '')
    multiplier = match.group(2).upper()
    
    try:
        num = float(num_str)
        if multiplier == 'K':
            num *= 1000
        elif multiplier == 'M':
            num *= 1000000
        return int(num)
    except ValueError:
        return 0


def find_influencers_serper(niche_keyword, location="", min_followers=10000, max_followers=100000, limit=20):
    """
    Replaces the Apify Hashtag Scraper. Uses the ultrafast Google Serper API
    to find thousands of profiles instantly using advanced Dorks.
    """
    logger.info(f"Serper Influencer Discovery: '{niche_keyword}' near '{location}' ({min_followers}-{max_followers} followers, limit={limit})")
    
    serper_key = _get_serper_api_key()
    url = "https://google.serper.dev/search"
    headers = {
        'X-API-KEY': serper_key,
        'Content-Type': 'application/json'
    }
    
    # ── 1. Create Google Dorks ──
    # -inurl: filters out specific posts, reels, tags, and explore pages so we only get root profiles
    # Examples:
    # site:instagram.com "fitness coach" india -inurl:p -inurl:reel -inurl:reels -inurl:explore -inurl:tags
    
    dork_query = f"site:instagram.com \"{niche_keyword}\""
    if location:
        dork_query += f" {location}"
        
    dork_query += " -inurl:p -inurl:reel -inurl:reels -inurl:explore -inurl:tags -inurl:stories"
    
    # We create multiple fallback variants. If Google runs out of pages on the first query
    # before we hit the limit, we swap to the next query and keep grabbing profiles.
    queries = [
        dork_query,
        dork_query.replace(f'"{niche_keyword}"', f'"{niche_keyword}" "startup"'),
        dork_query.replace(f'"{niche_keyword}"', f'"{niche_keyword}" "brand"'),
        dork_query.replace(f'"{niche_keyword}"', f'"{niche_keyword}" "store"')
    ]
    
    influencers = []
    seen_usernames = set()
    
    for current_dork in queries:
        if len(influencers) >= limit:
            break
            
        logger.info(f"Executing Dork: {current_dork}")
        
        # ── 2. Serper Pagination Loop ──
        page = 1
        
        while len(influencers) < limit and page <= 50: # Max 50 pages (5,000 results) to dig deep
            payload = {
                "q": current_dork,
                "page": page,
                "num": 50 # Serper supports num up to 100 on safe queries
            }
            
            # Add Geo-location parameter if researching India
            if location and "india" in location.lower():
                payload["gl"] = "in"
            
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=20)
                if response.status_code != 200:
                    logger.error(f"Serper API Error: {response.text}")
                    break
                    
                data = response.json()
                organic_results = data.get("organic", [])
                
                if not organic_results:
                    logger.warning(f"No more organic results found on page {page} for this query.")
                    break
                    
                for res in organic_results:
                    link = res.get("link", "")
                    snippet = res.get("snippet", "")
                    title = res.get("title", "")
                    
                    # Regex out the username
                    import re
                    match = re.search(r'instagram\.com/([a-zA-Z0-9_\.]+)', link)
                    if match:
                        username = match.group(1).strip('.')
                        skip = {"p", "reel", "reels", "explore", "stories", "accounts", "tags", "about", "directory"}
                        if username.lower() in skip or username.lower() in seen_usernames:
                            continue
                            
                        # Pre-filter: Check the Snippet for "14K Followers" to avoid wasting time verifying bad accounts
                        estimated_followers = _parse_followers_from_snippet(snippet)
                        
                        if estimated_followers > 0:
                            # If a snippet HAS a follower count, we strictly enforce it immediately
                            if estimated_followers < min_followers or estimated_followers > max_followers:
                                continue
                                
                        # Extract full name from the title (usually "Name (@username) • Instagram...")
                        full_name = title.split("(@")[0].split(" - ")[0].strip()
                        
                        seen_usernames.add(username.lower())
                        influencers.append({
                            "username": username,
                            "full_name": full_name,
                            "followers": estimated_followers or 0,
                            "following": 0,
                            "bio": snippet,
                            "profile_pic_url": "", # Serper doesn't provide high-res profile pics consistently
                            "engagement_rate": 0,  # Cannot calculate without Apify
                            "is_verified": False,
                            "category": "",
                            "external_url": ""
                        })
                        
                        if len(influencers) >= limit:
                            break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error calling Serper API: {e}")
                break
                
    logger.info(f"Serper Discovery COMPLETE: {len(influencers)} qualified influencers returned directly.")
    return influencers

