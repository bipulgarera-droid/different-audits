#!/usr/bin/env python3
"""
Instagram Scraper — Apify API integration
Fetches Instagram profile data, reels, and discovers competitors/influencers.

Actors used:
- instaprism/instagram-reels-scraper — Reels with video URLs, metrics, captions
- apify/instagram-profile-scraper — Profile basics (bio, followers, etc.)
- apify/google-search-scraper — Competitor discovery via Google (site:instagram.com search)
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
    items_resp = requests.get(items_url, params={"token": api_key, "format": "json"}, timeout=30)
    if items_resp.status_code == 200:
        items = items_resp.json()
        logger.info(f"Fetched {len(items)} items from dataset {dataset_id}")
        return items
    else:
        logger.error(f"Failed to fetch dataset: {items_resp.status_code}")
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


def find_influencers_by_niche(niche_keyword, location="", min_followers=10000, max_followers=100000, limit=20):
    """
    Discover influencers using Instagram Search API (not Google dorks).
    
    Step 1: Search Instagram directly for users matching the niche keyword.
            The search actor returns username + follower count, so we can filter
            BEFORE scraping full profiles (saves Apify credits).
    Step 2: Filter by follower range.
    Step 3: Scrape full profiles only for the filtered matches.
    """
    search_query = f"{niche_keyword} {location}".strip()
    
    logger.info(f"Influencer Discovery: Instagram search '{search_query}' ({min_followers}-{max_followers} followers)")
    
    # Step 1: Search Instagram directly for users
    search_results = _run_actor_async("apify/instagram-search-scraper", {
        "search": search_query,
        "searchType": "user",
        "resultsLimit": min(limit * 5, 100)  # Fetch extra to account for filtering
    }, timeout_secs=120)
    
    if not search_results:
        logger.warning("Instagram search returned no results, falling back to Google search")
        return _find_influencers_google_fallback(niche_keyword, location, min_followers, max_followers, limit)
    
    # Step 2: Filter by follower range using the search results
    candidates = []
    for item in search_results:
        username = item.get("username") or item.get("name", "")
        followers = item.get("followersCount", 0) or item.get("followers", 0) or 0
        
        if not username:
            continue
            
        # Skip system pages
        skip = {"p", "reel", "reels", "explore", "stories", "accounts", "tags", "about", "directory"}
        if username.lower() in skip:
            continue
        
        # Filter by follower range BEFORE scraping full profiles
        if followers < min_followers or followers > max_followers:
            logger.info(f"Skipping @{username} — {followers:,} followers (outside {min_followers:,}-{max_followers:,})")
            continue
        
        candidates.append({
            "username": username,
            "search_followers": followers,
            "full_name": item.get("fullName", "") or item.get("full_name", ""),
            "profile_pic_url": item.get("profilePicUrl", "") or item.get("profile_pic_url", ""),
            "bio": item.get("biography", "") or item.get("bio", ""),
            "is_verified": item.get("verified", False) or item.get("is_verified", False),
        })
    
    logger.info(f"Instagram search: {len(search_results)} results → {len(candidates)} match follower range")
    
    if not candidates:
        logger.warning("No candidates matched follower range, falling back to Google search")
        return _find_influencers_google_fallback(niche_keyword, location, min_followers, max_followers, limit)
    
    # Step 3: Scrape full profiles for the filtered candidates (more accurate data)
    target_usernames = [c["username"] for c in candidates[:min(limit * 2, 30)]]
    
    logger.info(f"Scraping full profiles for {len(target_usernames)} candidates...")
    profiles = _run_actor_async("apify/instagram-profile-scraper", {
        "usernames": target_usernames
    }, timeout_secs=180)
    
    influencers = []
    for item in profiles:
        username = item.get("username")
        if not username or item.get("error"):
            continue
        
        followers = item.get("followersCount", 0) or 0
        
        # Double-check follower range with actual profile data
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
    
    # Sort by engagement rate to surface the best prospects
    influencers.sort(key=lambda x: x["engagement_rate"], reverse=True)
    
    top = influencers[:limit]
    logger.info(f"Found {len(top)} qualified influencers matching {min_followers}-{max_followers} followers.")
    
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
    Skips the heavy Google Search / discovery phase.
    """
    logger.info(f"=== REEL OUTLIER ENGINE: Processing {len(competitors)} pre-found competitors ===")
    
    all_reels = []
    competitors_data = []
    
    # Process up to 'limit' competitors
    for comp in competitors[:limit]:
        uname = comp["username"]
        followers = comp.get("followers", 0)
        raw_profile = comp.get("raw", {})
        
        # Scrape their recent reels
        logger.info(f"Fetching recent reels for @{uname}...")
        
        # If the pre-found competitor was loaded from the DB, its "raw" data might have been 
        # cleaned out to save space. In that case, we need to perform a fresh scrape.
        if raw_profile and ("latestPosts" in raw_profile or "recentPosts" in raw_profile):
            reels = scrape_instagram_reels(uname, max_reels=30, profile_data={"raw": raw_profile}, recency_days=recency_days)
        else:
            logger.info(f"@{uname}: No pre-fetched posts in memory, scraping profile from Instagram...")
            reels = scrape_instagram_reels(uname, max_reels=30, recency_days=recency_days)
        
        
        if not reels:
            logger.warning(f"@{uname}: no reels found")
            continue
            
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
            
            # Outlier vs self
            outlier_vs_self = reel_eng / max(avg_eng, 1)
            
            # Engagement / follower ratio
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
    1. Discover top competitors via Instagram Search (returns full profiles + posts in ONE call)
    2. Extract ALL reels from their latestPosts
    3. Compute outlier score for each reel
    4. Return the top N outlier reels across all competitors
    
    Outlier score = (reel_engagement / competitor_avg_engagement) * (engagement / followers)
    """
    logger.info(f"=== COMPETITOR OUTLIER ENGINE START: {niche_keyword} in {location} ===")
    
    competitors = discover_competitors(niche_keyword, location, limit=limit)
    if not competitors:
        logger.warning(f"No competitors found for {niche_keyword} {location}")
        return []

    all_reels = []
    competitors_data = []
    
    for comp in competitors:
        uname = comp["username"]
        followers = comp["followers"]
        raw_profile = comp.get("raw", {})
        
        # The search scraper already returned latestPosts — reuse them directly!
        # No need for a second API call
        reels = scrape_instagram_reels(uname, max_reels=30, profile_data={"raw": raw_profile}, recency_days=recency_days)
        
        logger.info(f"@{uname}: {followers} followers, {len(reels)} reels with video content")
        
        if not reels:
            logger.warning(f"@{uname}: no reels found")
            continue
        
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
            
            # Outlier ratio: how much better is this reel vs the competitor's average?
            outlier_vs_self = reel_eng / max(avg_eng, 1)
            
            # Engagement-follower ratio: how well did this reel engage relative to audience size?
            eng_follower_ratio = (reel_eng / max(followers, 1)) * 100
            
            # Combined outlier score (higher = more viral outlier)
            outlier_score = outlier_vs_self * eng_follower_ratio
            
            reel["outlier_score"] = round(outlier_score, 2)
            reel["outlier_vs_self"] = round(outlier_vs_self, 2)
            reel["eng_follower_ratio"] = round(eng_follower_ratio, 2)
            reel["competitor"] = comp_info
            
            all_reels.append(reel)
    
    logger.info(f"Total reels collected: {len(all_reels)} from {len(competitors_data)} competitors")
    
    # Sort by outlier score descending — true viral outliers bubble to the top
    all_reels.sort(key=lambda x: x.get("outlier_score", 0), reverse=True)
    
    # Return the top N outliers
    top = all_reels[:top_reels]
    
    if top:
        logger.info(f"Top outlier: @{top[0].get('competitor', {}).get('username', '?')} — "
                     f"score={top[0]['outlier_score']}, views={top[0].get('views', 0)}")
    
    logger.info(f"=== COMPETITOR OUTLIER ENGINE COMPLETE: returning {len(top)} outlier reels ===")
    return top


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
