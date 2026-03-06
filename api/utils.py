from supabase import create_client
import os


def categorize_audit_issues(pages: list, summary: dict = None) -> dict:
    """
    Structure audit data into Accessibility, Usability, and Architecture categories.
    Returns counts and sample URLs for each metric.
    """
    if not pages:
        return {}

    # Initialize structure - FLATTENED for easier UI rendering
    data = {
        "accessibility": {
            "core_web_vitals": {"score": 0, "issues": 0, "label": "Pass"},
            "page_speed": {"score": 0, "issues": 0, "label": "Good"}, # Desktop/Mobile avg
            "mobile_friendly": {"issues": 0, "items": []},
            "image_alt_missing": {"issues": 0, "items": []},
            "images_large": {"issues": 0, "items": []}, # >100KB
            "web_dev_score": {"score": 0, "label": "N/A"}
        },
        "usability": {
            "title_missing": {"issues": 0, "items": []},
            "title_duplicate": {"issues": 0, "items": []},
            "title_over_65": {"issues": 0, "items": []},
            "desc_missing": {"issues": 0, "items": []},
            "desc_duplicate": {"issues": 0, "items": []},
            "desc_over_155": {"issues": 0, "items": []},
            "h1_missing": {"issues": 0, "items": []},
            "h1_multiple": {"issues": 0, "items": []},
            "h2_missing": {"issues": 0, "items": []},
            "low_word_count": {"issues": 0, "items": []},
            "misspelling": {"issues": 0, "items": []},
            "links_broken": {"issues": 0, "items": []},
            "links_redirect_3xx": {"issues": 0, "items": []},
            "orphan_urls": {"issues": 0, "items": []},
            "permalink_issues": {"issues": 0, "items": []},
            "sitemap_issues": {"issues": 0, "items": []},
            "robots_issues": {"issues": 0, "items": []},
            "no_canonical": {"issues": 0, "items": []},
            "duplicate_content": {"issues": 0, "items": []},
            "no_index": {"issues": 0, "items": []},
            "schema_missing": {"issues": 0, "items": []},
            "server_errors_5xx": {"issues": 0, "items": []},
            "client_errors_4xx": {"issues": 0, "items": []},
        },
        "architecture": {
            "site_architecture": {"issues": 0, "items": [], "label": "Coming Soon"},
        }
    }

    # Aggregate Data
    total_speed_score = 0
    pages_with_speed = 0
    
    # CWV Counters
    cwv_issues = 0
    
    for page in pages:
        url = page.get('url', '')
        meta = page.get('meta', {})
        
        # Robust Check Extraction
        checks = page.get('checks', {}) 
        if not checks:
             checks = page.get('dfs_checks', {})
        if not checks:
             checks = page.get('issues', {})

        content = page.get('content', {})
        
        # --- Accessibility ---
        
        # Core Web Vitals (Simple Logic)
        # Fail if LCP > 2.5s OR CLS > 0.1
        lcp = page.get('largest_contentful_paint', 0) or 0
        cls = page.get('cumulative_layout_shift', 0) or 0
        if lcp > 2500 or cls > 0.1:
            cwv_issues += 1
            # Add to items so it's clickable
            data['accessibility']['core_web_vitals'].setdefault('items', []).append(f"{url} (LCP: {lcp}ms, CLS: {cls})")

        # Mobile Friendly
        if checks.get('is_mobile_friendly') is False:
             data['accessibility']['mobile_friendly']['issues'] += 1
             data['accessibility']['mobile_friendly']['items'].append(url)

        # Image Alt
        if checks.get('no_image_alt'):
             data['accessibility']['image_alt_missing']['issues'] += 1
             data['accessibility']['image_alt_missing']['items'].append(url)

        # Broken Images / Resources (New Check to capture missing assets)
        if checks.get('broken_resources') or checks.get('has_broken_resources'):
             data['accessibility']['images_large']['issues'] += 1 # Grouping with large images or create new? Using existing Large Images for "Asset Issues"
             data['accessibility']['images_large']['items'].append(f"{url} (Broken Resource)")

        # Large Images (>100KB avg)
        if (page.get('images_size', 0) / (page.get('images_count', 1) or 1)) > 102400: 
             data['accessibility']['images_large']['issues'] += 1
             data['accessibility']['images_large']['items'].append(url)

        # Speed (OnPage Score as proxy)
        score = page.get('onpage_score', 0)
        if score:
            total_speed_score += score
            pages_with_speed += 1
            
        # Flag poor speed pages
        if score < 50:
             data['accessibility']['page_speed']['issues'] += 1
             data['accessibility']['page_speed'].setdefault('items', []).append(f"{url} (Score: {score})")

        # --- Usability ---

        # Titles
        title = meta.get('title', '')
        if not title:
            data['usability']['title_missing']['issues'] += 1
            data['usability']['title_missing']['items'].append(url)
        elif len(title) > 65:
            data['usability']['title_over_65']['issues'] += 1
            data['usability']['title_over_65']['items'].append(url)
        
        if checks.get('duplicate_title') or checks.get('duplicate_title_tag'):
             data['usability']['title_duplicate']['issues'] += 1
             data['usability']['title_duplicate']['items'].append(url)

        # Descriptions
        desc = meta.get('description', '')
        if not desc:
            data['usability']['desc_missing']['issues'] += 1
            data['usability']['desc_missing']['items'].append(url)
        elif len(desc) > 155:
            data['usability']['desc_over_155']['issues'] += 1
            data['usability']['desc_over_155']['items'].append(url)
            
        if checks.get('duplicate_description'):
             data['usability']['desc_duplicate']['issues'] += 1
             data['usability']['desc_duplicate']['items'].append(url)

        # Headings
        if checks.get('no_h1') or checks.get('no_h1_tag'):
             data['usability']['h1_missing']['issues'] += 1
             data['usability']['h1_missing']['items'].append(url)
        if checks.get('duplicate_h1') or checks.get('duplicate_h1_tag') or len(meta.get('h1', []) or []) > 1:
             data['usability']['h1_multiple']['issues'] += 1
             data['usability']['h1_multiple']['items'].append(url)
             
        if len(meta.get('h2', []) or []) == 0:
             data['usability']['h2_missing']['issues'] += 1
             data['usability']['h2_missing']['items'].append(url)

        # Content
        if checks.get('low_content') or checks.get('low_content_rate'):
             data['usability']['low_word_count']['issues'] += 1
             data['usability']['low_word_count']['items'].append(url)

        # Misspelling
        if checks.get('has_misspelling'):
             data['usability']['misspelling']['issues'] += 1
             data['usability']['misspelling']['items'].append(url)
             
        # Links
        # Catch explicit 4xx/5xx pages AND pages with broken links on them
        if checks.get('is_broken') or checks.get('broken_links') or checks.get('has_broken_links'):
             data['usability']['links_broken']['issues'] += 1
             data['usability']['links_broken']['items'].append(url)
        if checks.get('is_redirect'):
             data['usability']['links_redirect_3xx']['issues'] += 1
             data['usability']['links_redirect_3xx']['items'].append(url)
        if checks.get('is_orphan_page'):
             data['usability']['orphan_urls']['issues'] += 1
             data['usability']['orphan_urls']['items'].append(url)


        # --- Architecture (Moved to Usability per 3-pillar model) ---
        
        # Permalink Structure (SEO Friendly URL)
        if checks.get('seo_friendly_url') is False:
             data['usability']['permalink_issues']['issues'] += 1
             data['usability']['permalink_issues']['items'].append(url)

        # Indexing
        if checks.get('no_canonical'):
             data['usability']['no_canonical']['issues'] += 1
             data['usability']['no_canonical']['items'].append(url)
        if checks.get('duplicate_content'):
             data['usability']['duplicate_content']['issues'] += 1
             data['usability']['duplicate_content']['items'].append(url)
        if checks.get('is_marked_as_noindex') or checks.get('no_index'): 
             data['usability']['no_index']['issues'] += 1
             data['usability']['no_index']['items'].append(url)
             
        # Server
        if page.get('status_code', 200) >= 500:
             data['usability']['server_errors_5xx']['issues'] += 1
             data['usability']['server_errors_5xx']['items'].append(url)
        elif page.get('status_code', 200) >= 400:
             data['usability']['client_errors_4xx']['issues'] += 1
             data['usability']['client_errors_4xx']['items'].append(url)

        # Schema (Basic check)
        if not page.get('meta', {}).get('schema') and not checks.get('has_schema'):
             data['usability']['schema_missing']['issues'] += 1
             data['usability']['schema_missing']['items'].append(url)


    # Summary Level Data overrides/calculations
    if summary:
        data['accessibility']['page_speed']['score'] = int(summary.get('onpage_score', 0))
        data['accessibility']['web_dev_score']['score'] = int(summary.get('onpage_score', 0)) # Proxy
        
        # Sitemap Check from Summary
        if not summary.get('has_sitemap'):
            data['usability']['sitemap_issues']['issues'] = 1
            data['usability']['sitemap_issues']['items'].append("Sitemap missing")

        # Robots.txt Check (Proxy: if we crawled, it's likely accessible, but check summary logic if available)
        # For now, we leave 0 unless specific error
            
    elif pages_with_speed > 0:
        avg_score = int(total_speed_score / pages_with_speed)
        data['accessibility']['page_speed']['score'] = avg_score
        data['accessibility']['web_dev_score']['score'] = avg_score

    # CWV Score Calculation
    data['accessibility']['core_web_vitals']['issues'] = cwv_issues
    if cwv_issues == 0 and len(pages) > 0:
        data['accessibility']['core_web_vitals']['label'] = "Pass"
        data['accessibility']['core_web_vitals']['score'] = 100
    else:
        data['accessibility']['core_web_vitals']['label'] = "Fail"
        data['accessibility']['core_web_vitals']['score'] = max(0, 100 - (cwv_issues * 5))

    # Logic for Labels
    score = data['accessibility']['page_speed']['score']
    if score >= 90:
        data['accessibility']['page_speed']['label'] = "Excellent"
    elif score >= 50:
        data['accessibility']['page_speed']['label'] = "Fair"
    else:
        data['accessibility']['page_speed']['label'] = "Poor"

    # Set status labels for all items
    for cat in data:
        for key in data[cat]:
            item = data[cat][key]
            # Use get() safely
            if item.get('issues', 0) > 0:
                item['status'] = 'fail'
            elif item.get('score') is not None and item.get('score') < 50:
                item['status'] = 'fail'
            else:
                item['status'] = 'pass'

    return data
