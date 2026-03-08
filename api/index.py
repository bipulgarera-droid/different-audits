#!/usr/bin/env python3
"""
SEO Agency Platform - Main API
Flask application with role-based authentication and multi-tenant support.
"""
import os
import sys

# Add local libs to path (for openpyxl)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'libs')))
import logging
from datetime import datetime
from flask import Flask, jsonify, request, render_template, redirect, url_for, session, send_file
from flask_cors import CORS
from dotenv import load_dotenv, dotenv_values
from functools import wraps
from api.dataforseo_client import (
    start_onpage_audit,
    get_audit_status,
    get_audit_summary,
    get_page_issues,
    get_domain_rank_overview
)
from api.utils import categorize_audit_issues
from api.export import generate_audit_excel
from execution.screenshot_capture import capture_screenshot_with_fallback
from api.deep_audit_slides import create_deep_audit_slides
from api.google_auth import get_google_credentials
from execution.instagram_scraper import (
    scrape_instagram_profile,
    scrape_instagram_reels,
    discover_competitors
)
from execution.video_transcriber import batch_transcribe_reels
from execution.hook_extractor import extract_hooks_batch
import threading
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# import check
from pathlib import Path

# Initialize Flask
# Use pathlib for better handling of spaces in paths
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables
env_local_path = BASE_DIR / '.env.local'
env_path = BASE_DIR / '.env'

# 1. Try dotenv_values which handles file paths nicely
try:
    logger.info("Attempting to load variables from .env...")
    config = dotenv_values(str(env_path))
    if not config:
        logger.info("dict from .env was empty, trying plain .env in current dir")
        config = dotenv_values(".env")
        
    for k, v in config.items():
        if k and v and not os.getenv(k):
            os.environ[k] = v
            
    if os.getenv('SUPABASE_URL'):
        logger.info("Successfully loaded SUPABASE from .env!")
    else:
        logger.warning("Could not find SUPABASE in config after dotenv_values.")
except Exception as e:
    logger.error(f"Error loading environment via dotenv_values: {e}")

# Verify critical vars
if not os.getenv('SUPABASE_URL'):
    logger.warning("CRITICAL: SUPABASE_URL not found in environment!")
else:
    logger.info("Supabase URL configured.")

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / 'public'),
    static_folder=str(BASE_DIR / 'public'),
    static_url_path=''
)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')
CORS(app)

# Register Google OAuth Integration Blueprint
try:
    from api.google_integration import google_integration_bp
    app.register_blueprint(google_integration_bp)
except Exception as e:
    logger.error(f"Failed to register google_integration blueprint: {e}")

# Supabase client
from supabase import create_client, Client

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

logger.info(f"Keys found: URL={'yes' if SUPABASE_URL else 'no'}, ANON={'yes' if SUPABASE_KEY else 'no'}, SERVICE={'yes' if SUPABASE_SERVICE_KEY else 'no'}")

supabase: Client = None
supabase_admin: Client = None

# Prefer service role key for the main client to bypass RLS
effective_key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

if SUPABASE_URL and effective_key:
    supabase = create_client(SUPABASE_URL, effective_key)
    if SUPABASE_SERVICE_KEY:
        supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    else:
        # If service role key not available, use anon for admin too
        supabase_admin = supabase
    logger.info(f"Supabase client initialized (main uses {'service_role' if SUPABASE_SERVICE_KEY else 'anon'} key)")
else:
    logger.warning("Supabase credentials not found - running without database")

# =============================================================================
# LOCAL DEV AUTH BYPASS
# =============================================================================
@app.before_request
def bypass_auth_for_local():
    """Bypass auth by injecting a dummy admin user into the session."""
    if 'user' not in session:
        # We need an organization ID so campaigns load
        org_id = None
        client = supabase_admin or supabase
        if client:
            try:
                # Get the first org
                orgs = client.table('organizations').select('id').limit(1).execute()
                if orgs.data:
                    org_id = orgs.data[0]['id']
                else:
                    # Create a dummy org
                    new_org = client.table('organizations').insert({
                        'name': 'Local Dev Org', 
                        'slug': 'local-dev-org'
                    }).execute()
                    if new_org.data:
                        org_id = new_org.data[0]['id']
            except Exception as e:
                logger.error(f"Failed to fetch/create dummy organization: {e}")
        
        session['user'] = {
            'id': '00000000-0000-0000-0000-000000000000',
            'email': 'local@different-audits.com',
            'role': 'admin',
            'organization_id': org_id
        }

# =============================================================================
# ROLE DEFINITIONS
# =============================================================================

ROLES = {
    'admin': {
        'name': 'Administrator',
        'permissions': ['all']
    },
    'campaign_manager': {
        'name': 'Campaign Manager',
        'permissions': ['view_all_campaigns', 'assign_tasks', 'view_reports', 'manage_team']
    },
    'content_strategist': {
        'name': 'Content Strategist',
        'permissions': ['view_campaigns', 'manage_keywords', 'manage_content_calendar', 'create_briefs']
    },
    'content_creator': {
        'name': 'Content Creator',
        'permissions': ['view_assigned_tasks', 'create_content', 'submit_drafts']
    },
    'optimization_specialist': {
        'name': 'Optimization Specialist',
        'permissions': ['view_assigned_tasks', 'view_audits', 'fix_issues']
    },
    'link_builder': {
        'name': 'Link Builder',
        'permissions': ['view_assigned_tasks', 'manage_links', 'track_placements']
    },
    'reporting_manager': {
        'name': 'Reporting Manager',
        'permissions': ['view_all_campaigns', 'create_reports', 'export_data']
    },
    'viewer': {
        'name': 'Client Viewer',
        'permissions': ['view_own_campaign']
    }
}

# =============================================================================
# AUTH DECORATORS
# =============================================================================

def login_required(f):
    """Require user to be logged in."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function


def role_required(*roles):
    """Require user to have one of the specified roles."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                return jsonify({'error': 'Authentication required'}), 401
            user_role = session.get('user', {}).get('role', 'viewer')
            if user_role not in roles and user_role != 'admin':
                return jsonify({'error': 'Insufficient permissions'}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def permission_required(permission):
    """Require user to have a specific permission."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user' not in session:
                return jsonify({'error': 'Authentication required'}), 401
            user_role = session.get('user', {}).get('role', 'viewer')
            role_perms = ROLES.get(user_role, {}).get('permissions', [])
            if 'all' not in role_perms and permission not in role_perms:
                return jsonify({'error': 'Insufficient permissions'}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.route('/ping')
def ping():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'message': 'SEO Agency Platform API',
        'supabase_connected': supabase is not None
    })


@app.route('/')
def index():
    """Serve main page."""
    if 'user' in session:
        return redirect('/dashboard')
    return render_template('login.html')


@app.route('/dashboard')
@login_required
def dashboard():
    """Serve dashboard based on user role."""
    return render_template('dashboard.html')

@app.route('/audit-dashboard.html')
@login_required
def audit_dashboard():
    """Serve the advanced deep audit dashboard."""
    return render_template('audit-dashboard.html')

# =============================================================================
# AUTH ROUTES
# =============================================================================

@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login with email/password via Supabase."""
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    
    if supabase is None:
        logger.error("Supabase client not initialized")
        return jsonify({'error': 'Database connection error. Please check server logs.'}), 500
    
    try:
        # Authenticate with Supabase
        response = supabase.auth.sign_in_with_password({
            'email': email,
            'password': password
        })
        
        user = response.user
        
        # Get user profile with role
        profile = supabase.table('profiles').select('*').eq('id', user.id).single().execute()
        
        # BACKFILL: If user has no organization, create one now
        if profile.data and not profile.data.get('organization_id'):
            try:
                # Reuse creation logic
                full_name = profile.data.get('full_name') or user.email.split('@')[0]
                org_name = f"{full_name}'s Org"
                slug = org_name.lower().replace(' ', '-').replace("'", "") + f"-{int(datetime.now().timestamp())}"
                
                admin = supabase_admin or supabase
                org_res = admin.table('organizations').insert({
                    'name': org_name,
                    'slug': slug,
                    'owner_id': user.id
                }).execute()
                
                if org_res.data:
                    org_id = org_res.data[0]['id']
                    
                    # 2. Update Profile with Org ID
                    updated_profile = admin.table('profiles').update({
                        'organization_id': org_id,
                        'role': 'admin'
                    }).eq('id', user.id).execute()
                    
                    # 3. MIGRATION: Adopt orphaned campaigns (Safe heuristics)
                    # If this is the "main" user (or first to migrate), give them the legacy data
                    # We check if this user effectively "owns" the legacy state
                    # For simplicity/safety in this specific context: Update ALL null-org campaigns
                    migration_res = admin.table('campaigns').update({'organization_id': org_id}).is_('organization_id', 'null').execute()
                    if migration_res.data:
                        logger.info(f"Migrated {len(migration_res.data)} orphaned campaigns to org {org_id}")

                    # Use updated profile data
                    if updated_profile.data:
                        profile = updated_profile
                        logger.info(f"Backfilled organization {org_id} for user {user.id}")
            except Exception as e:
                logger.error(f"Failed to backfill org for {user.email}: {e}")
        
        # Store in session
        session['user'] = {
            'id': user.id,
            'email': user.email,
            'role': profile.data.get('role', 'viewer') if profile.data else 'viewer',
            'organization_id': profile.data.get('organization_id') if profile.data else None,
            'full_name': profile.data.get('full_name') if profile.data else None
        }
        session['access_token'] = response.session.access_token
        
        return jsonify({
            'success': True,
            'user': session['user'],
            'role_info': ROLES.get(session['user']['role'], {})
        })
        
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({'error': str(e)}), 401

        return jsonify({'error': str(e)}), 401


@app.route('/api/auth/change-password', methods=['POST'])
@login_required
def change_password():
    """Change user password."""
    data = request.json
    current_password = data.get('current_password')
    new_password = data.get('new_password')
    
    if not current_password or not new_password:
        return jsonify({'error': 'Current and new password required'}), 400

    try:
        # 1. Verify current password
        user_email = session['user']['email']
        
        # We try to sign in. If it fails, current password is wrong.
        # Note: This might create a new session on Supabase side, but that's fine.
        auth_res = supabase.auth.sign_in_with_password({
            'email': user_email,
            'password': current_password
        })
        
        if not auth_res.user:
            return jsonify({'error': 'Incorrect current password'}), 401

        # 2. Update password
        # users.update() updates the user.
        update_res = supabase.auth.update_user({
            'password': new_password
        })
        
        if update_res:
             return jsonify({'success': True, 'message': 'Password updated successfully'})
        else:
             return jsonify({'error': 'Failed to update password'}), 500

    except Exception as e:
        logger.error(f"Password change error: {e}")
        # HACK: Supabase/GoTrue specific error messages often come in e.message or str(e)
        msg = str(e)
        if "Invalid login credentials" in msg:
             return jsonify({'error': 'Incorrect current password'}), 401
        return jsonify({'error': f"Failed to change password: {msg}"}), 500


@app.route('/api/auth/signup', methods=['POST'])
def signup():
    """Register new user."""
    data = request.json
    email = data.get('email')
    password = data.get('password')
    full_name = data.get('full_name', '')
    
    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    
    try:
        # Create user in Supabase Auth with metadata
        response = supabase.auth.sign_up({
            'email': email,
            'password': password,
            'options': {
                'data': {
                    'full_name': full_name
                }
            }
        })
        
        user = response.user
        
        if not user:
            return jsonify({'error': 'Signup failed. Please try again.'}), 400
        
        # Profile is created automatically by trigger
        # NOW: Create Organization and assign it (Critical for data isolation)
        try:
            # Generate basic slug
            org_name = f"{full_name}'s Org" if full_name else "My Organization"
            slug = org_name.lower().replace(' ', '-').replace("'", "") + f"-{int(datetime.now().timestamp())}"
            
            # Use admin client to ensure we can create orgs and update profiles
            admin = supabase_admin or supabase
            
            # 1. Create Org
            org_res = admin.table('organizations').insert({
                'name': org_name,
                'slug': slug,
                'owner_id': user.id
            }).execute()
            
            if org_res.data:
                org_id = org_res.data[0]['id']
                
                # 2. Update Profile with Org ID
                admin.table('profiles').update({
                    'organization_id': org_id,
                    'role': 'admin' # First user is admin of their org
                }).eq('id', user.id).execute()
                
                logger.info(f"Created organization {org_id} for new user {user.id}")
                
        except Exception as e:
            logger.error(f"Failed to auto-create org for {email}: {e}")
            # Don't fail the whole signup, but log it. User will be caught by Login backfill.

        return jsonify({
            'success': True,
            'message': 'Account created! You can now sign in.'
        })
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Signup error: {error_msg}")
        
        # Parse common errors into user-friendly messages
        if 'already registered' in error_msg.lower() or 'already exists' in error_msg.lower():
            return jsonify({'error': 'An account with this email already exists. Please sign in.'}), 400
        elif 'duplicate key' in error_msg.lower() or 'profiles_pkey' in error_msg.lower():
            return jsonify({'error': 'Account already exists. Please sign in instead.'}), 400
        elif 'password' in error_msg.lower():
            return jsonify({'error': 'Password must be at least 6 characters.'}), 400
        else:
            return jsonify({'error': 'Signup failed. Please try again.'}), 400


@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout user."""
    session.clear()
    return jsonify({'success': True})


@app.route('/api/auth/me')
@login_required
def get_current_user():
    """Get current user info."""
    return jsonify({
        'user': session.get('user'),
        'role_info': ROLES.get(session['user']['role'], {})
    })

# =============================================================================
# ORGANIZATION ROUTES
# =============================================================================

@app.route('/api/organizations', methods=['GET'])
@login_required
@role_required('admin')
def list_organizations():
    """List all organizations (admin only)."""
    try:
        response = supabase.table('organizations').select('*').execute()
        return jsonify({'organizations': response.data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/organizations', methods=['POST'])
@login_required
@role_required('admin')
def create_organization():
    """Create new organization."""
    data = request.json
    
    try:
        response = supabase.table('organizations').insert({
            'name': data.get('name'),
            'slug': data.get('slug'),
            'owner_id': session['user']['id']
        }).execute()
        
        return jsonify({'organization': response.data[0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================================================
# CAMPAIGN ROUTES
# =============================================================================

@app.route('/api/campaigns', methods=['GET'])
@login_required
def list_campaigns():
    """List campaigns visible to user."""
    user = session['user']
    
    # Use admin client to bypass RLS (backend handles authorization)
    client = supabase_admin or supabase
    
    try:
        query = client.table('campaigns').select('*')
        
        # Filter by organization for EVERYONE (Admin means Org Admin, not Superuser)
        if user.get('organization_id'):
            query = query.eq('organization_id', user['organization_id'])
        else:
            # If no org ID (e.g. local dev bypass failed org creation), don't break the UI
            pass
        
        response = query.order('created_at', desc=True).execute()
        return jsonify({'campaigns': response.data})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/campaigns', methods=['POST'])
@login_required
@permission_required('view_all_campaigns')
def create_campaign():
    """Create new campaign."""
    data = request.json
    user = session['user']
    
    # Use admin client for write operations (bypasses RLS)
    client = supabase_admin or supabase
    
    try:
        response = client.table('campaigns').insert({
            'organization_id': user.get('organization_id'),
            'name': data.get('name'),
            'domain': data.get('domain'),
            'settings': data.get('settings', {}),
            'status': 'active'
        }).execute()
        
        return jsonify({'campaign': response.data[0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/campaigns/<campaign_id>', methods=['GET'])
@login_required
def get_campaign(campaign_id):
    """Get single campaign."""
    client = supabase_admin or supabase
    try:
        response = client.table('campaigns').select('*').eq('id', campaign_id).single().execute()
        return jsonify({'campaign': response.data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/campaigns/<campaign_id>', methods=['PUT'])
@login_required
@permission_required('view_all_campaigns')
def update_campaign(campaign_id):
    """Update campaign."""
    data = request.json
    client = supabase_admin or supabase
    
    # Only include fields that are provided
    update_data = {}
    if 'name' in data:
        update_data['name'] = data['name']
    if 'domain' in data:
        update_data['domain'] = data['domain']
    if 'settings' in data:
        update_data['settings'] = data['settings']
    if 'status' in data:
        update_data['status'] = data['status']
    
    if not update_data:
        return jsonify({'error': 'No fields to update'}), 400
    
    try:
        response = client.table('campaigns').update(update_data).eq('id', campaign_id).execute()
        return jsonify({'campaign': response.data[0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================================================
# AUDIT ROUTES
# =============================================================================

@app.route('/api/audits', methods=['GET'])
@login_required
def list_audits():
    """List audits for user's campaigns."""
    campaign_id = request.args.get('campaign_id')
    audit_type = request.args.get('type') # Expected: 'technical' or 'competitor'
    
    try:
        # Use admin client to bypass RLS or ensure context
        client = supabase_admin or supabase
        user = session['user']
        
        # Join campaigns to filter by Org
        query = client.table('audits').select('*, campaigns!inner(name, domain, organization_id)')
        
        # KEY FIX: Filter by Organization
        if user.get('organization_id'):
             query = query.eq('campaigns.organization_id', user['organization_id'])
        else:
             return jsonify({'audits': []})

        if campaign_id:
            query = query.eq('campaign_id', campaign_id)
            
        if audit_type:
            query = query.eq('type', audit_type)
        
        response = query.order('created_at', desc=True).execute()
        return jsonify({'audits': response.data})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/audits/<audit_id>/generate-slides', methods=['POST'])
@login_required
@permission_required('view_all_audits')
def generate_audit_slides(audit_id):
    """Generate Google Slides for an audit."""
    user = session['user']
    
    try:
        # Get audit data
        audit = supabase_admin.table('audits').select('*').eq('id', audit_id).execute()
        if not audit.data:
            return jsonify({'error': 'Audit not found'}), 404
            
        audit_data = audit.data[0]
        
        # Check permissions (basic organization check)
        if user['role'] != 'admin' and audit_data.get('organization_id') != user.get('organization_id'):
             return jsonify({'error': 'Unauthorized'}), 403

        # Check if already has slides? (Optional: allow regeneration)
        
        # Import generator here to avoid circular imports or early failures if dependencies missing
        try:
            from api.deep_audit_slides import create_deep_audit_slides
        except ImportError as e:
            return jsonify({'error': f'Slides generator module error: {str(e)}'}), 500

        # Run generation
        # Note: This can take time. Ideally should be a background task (Celery/RQ).
        # For now, running synchronously but it might timeout on Vercel/Railway if > 30s.
        # We'll assume it's fast enough or user accepts wait.
        
        full_data = audit_data.get('data', {})
        # If competitor audit, use that domain for the slides title
        results_obj = audit_data.get('results') or {}
        domain = results_obj.get('competitor_domain') or audit_data.get('settings', {}).get('domain') or 'Website'
        
        try:
            result = create_deep_audit_slides(full_data, domain)
            slides_url = result.get('presentation_url')
            
            # Update audit record
            supabase.table('audits').update({'slides_url': slides_url}).eq('id', audit_id).execute()
            
            return jsonify({'slides_url': slides_url})
            
        except FileNotFoundError as e:
            # Likely missing credentials or asset
            return jsonify({'error': f'File not found error: {str(e)}'}), 503
        except Exception as e:
            return jsonify({'error': f'Failed to generate slides: {str(e)}'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/audits', methods=['POST'])
@login_required
@permission_required('view_all_campaigns')
def create_audit():
    """Start a new audit."""
    data = request.json
    
    # Use admin client for write operations
    client = supabase_admin or supabase
    
    try:
        # Get campaign domain
        campaign = client.table('campaigns').select('domain').eq('id', data.get('campaign_id')).single().execute()
        if not campaign.data:
            return jsonify({'error': 'Campaign not found'}), 404
            
        target_domain = data.get('competitor_domain') or campaign.data['domain']
        audit_type = data.get('type', 'technical')
        
        try:
            max_pages = int(data.get('max_pages', 200))
        except (ValueError, TypeError):
            max_pages = 200
        
        # Start DataForSEO audit
        dfs_result = start_onpage_audit(target_domain, max_pages=max_pages)
        
        if not dfs_result.get('success'):
            return jsonify({'error': f"Failed to start audit: {dfs_result.get('error')}"}), 500
            
        task_id = dfs_result.get('task_id')

        # Create audit record
        # If it's a competitor, persist the competitor_domain in the results dict so UI can show it
        initial_results = {}
        if data.get('competitor_domain'):
            initial_results['competitor_domain'] = data.get('competitor_domain')
            
        if data.get('template_type'):
            initial_results['template_type'] = data.get('template_type')

        response = client.table('audits').insert({
            'campaign_id': data.get('campaign_id'),
            'type': audit_type,
            'status': 'crawling',
            'dataforseo_task_id': task_id,
            'results': initial_results
        }).execute()
        
        audit = response.data[0]
        audit_id = audit['id']
        
        # ---- DUAL WRITE: Also create a projects record for audit-dashboard.html ----
        try:
            # Fetch keywords + backlinks in parallel with crawl (same as audit-app)
            from api.dataforseo_client import fetch_ranked_keywords, fetch_backlinks_summary, get_referring_domains
            
            keywords_data = fetch_ranked_keywords(target_domain)
            keywords = keywords_data.get('keywords', []) if isinstance(keywords_data, dict) else []
            keywords_total_count = keywords_data.get('total_count', len(keywords))
            keywords_estimated_traffic = keywords_data.get('estimated_traffic', 0)
            keywords_at_limit = keywords_data.get('keywords_at_limit', len(keywords) >= 1000)
            
            backlinks_summary = fetch_backlinks_summary(target_domain)
            referring_domains = get_referring_domains(target_domain)
            
            import time as time_mod
            full_audit_data = {
                'task_id': task_id,
                'domain': target_domain,
                'status': 'pending',
                'created_at': time_mod.strftime("%Y-%m-%dT%H:%M:%SZ"),
                'organic_keywords': keywords,
                'total_keywords': keywords_total_count,
                'total_traffic': keywords_estimated_traffic,
                'keywords_at_limit': keywords_at_limit,
                'backlinks_summary': backlinks_summary,
                'referring_domains': referring_domains,
                'max_pages': max_pages
            }
            
            project_response = client.table('projects').insert({
                'domain': target_domain,
                'full_audit_data': full_audit_data,
                'source': 'agency-platform',
                'audit_id': audit_id
            }).execute()
            
            project_id = project_response.data[0]['id'] if project_response.data else None
            logger.info(f"Dual-write: created project {project_id} for audit {audit_id}")
        except Exception as dual_err:
            logger.error(f"Dual-write to projects failed (non-fatal): {dual_err}")
        # ---- END DUAL WRITE ----
        
        return jsonify({'audit': audit, 'message': 'Audit started successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/audits/<audit_id>', methods=['GET'])
@login_required
def get_audit(audit_id):
    """Get audit status and results."""
    try:
        client = supabase_admin or supabase
        response = client.table('audits').select('*, campaigns(name, domain)').eq('id', audit_id).single().execute()
        audit = response.data
        
        # Lazy status check for running audits
        if audit['status'] == 'crawling' and audit.get('dataforseo_task_id'):
            task_id = audit['dataforseo_task_id']
            status = get_audit_status(task_id)
            
            if status.get('ready'):
                # Audit finished! Fetch results and update
                try:
                    # 1. Get Summary
                    summary = get_audit_summary(task_id)
                    
                    # 2. Get Page Issues
                    pages_result = get_page_issues(task_id, limit=100)
                    pages = pages_result.get('pages', [])
                    
                    # 3. Categorize Results for UI (First, so we can use for tasks)
                    categorized = categorize_audit_issues(pages, summary.get('summary'))
                    
                    # 4. Create Tasks (Removed)
                    
                    # 5. Update Audit Record
                    update_data = {
                        'status': 'completed',
                        'results': {
                            'summary': summary.get('summary', {}),
                            'categorized': categorized,
                            'pages': pages
                        }
                    }
                    
                    update_res = client.table('audits').update(update_data).eq('id', audit_id).execute()
                    audit = update_res.data[0] # Return updated audit
                    
                except Exception as e:
                     print(f"Error finalizing audit: {e}")
                     # Optional: fail status
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # ---------------------------------------------------------
    # LEGACY DATA SUPPORT: On-the-fly migration
    # ---------------------------------------------------------
    if audit.get('status') == 'completed' and audit.get('results'):
        results = audit['results']
        categorized = results.get('categorized')
        
        # Check if migration is needed:
        # 1. No categorized data at all (very old)
        # 2. Old categorization (Architecture contains items that should be in Usability)
        needs_migration = False
        
        if not categorized and 'pages' in results:
            needs_migration = True
        elif categorized and 'architecture' in categorized:
            # Check for a key that Moved, e.g., 'server_errors_5xx'
            if 'server_errors_5xx' in categorized['architecture']:
                needs_migration = True
        
        if needs_migration and 'pages' in results:
            try:
                # print(f"Migrating legacy audit {audit['id']} on the fly...")
                new_categorized = categorize_audit_issues(results['pages'], results.get('summary'))
                audit['results']['categorized'] = new_categorized
                
                # Persist the migration
                (supabase_admin or supabase).table('audits').update({
                    'results': audit['results']
                }).eq('id', audit['id']).execute()
                # print("Migration persisted.")
            except Exception as e:
                print(f"Failed to migrate legacy audit: {e}")

    # Build response with success flag and flattened fields for audit-dashboard.html
    results = audit.get('results', {}) or {}
    campaign_data = audit.get('campaigns', {}) or {}
    domain = results.get('competitor_domain') or campaign_data.get('domain', '')
    
    flat_audit = {
        **audit,
        'domain': domain,
        'keywords': results.get('keywords', []),
        'pages': results.get('pages', []),
        'pagespeed': results.get('pagespeed', {}),
        'backlinks': results.get('backlinks', {}),
        'backlinks_summary': results.get('backlinks_summary', results.get('backlinks', {})),
        'referring_domains': results.get('referring_domains', []),
        'total_keywords': results.get('total_keywords', 0),
        'total_traffic': results.get('total_traffic', 0),
        'keywords_at_limit': results.get('keywords_at_limit', 0)
    }
    
    return jsonify({'success': True, 'audit': flat_audit})

@app.route('/api/audits/<audit_id>/export', methods=['GET'])
@login_required
def export_audit(audit_id):
    """Export audit result as Excel."""
    try:
        client = supabase_admin or supabase
        response = client.table('audits').select('*, campaigns(name, domain)').eq('id', audit_id).single().execute()
        audit = response.data
        
        # Ensure we have results to export
        if not audit.get('results'):
            return jsonify({'error': 'Audit has no results to export'}), 400
            
        # Migrate if needed (reuse logic or just trust current state)
        # Ideally, we should unify the read logic, but for export we just take what's there
        # If categorized data is missing, we might want to run categorization on the fly here too
        results = audit['results']
        if 'categorized' not in results and 'pages' in results:
             results['categorized'] = categorize_audit_issues(results['pages'], results.get('summary'))
        
        # Generate Excel
        output = generate_audit_excel(audit)
        
        filename = f"audit_report_{audit.get('campaigns', {}).get('domain', 'site')}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Export failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/audits/list', methods=['GET'])
@login_required
def list_audits_for_dashboard():
    """List audits for the audit-dashboard selector dropdown."""
    try:
        client = supabase_admin or supabase
        response = client.table('audits').select('id, created_at, status, results, campaigns(domain)').order('created_at', desc=True).limit(50).execute()
        audits = []
        for a in response.data:
            campaign_data = a.get('campaigns', {}) or {}
            results = a.get('results', {}) or {}
            domain = results.get('competitor_domain') or campaign_data.get('domain', 'Unknown')
            audits.append({
                'id': a['id'],
                'domain': domain,
                'created_at': a['created_at'],
                'status': a['status']
            })
        return jsonify({'audits': audits})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/deep-audit/status/<task_id>', methods=['GET'])
@login_required
def deep_audit_status(task_id):
    """Check DataForSEO crawl status for the deep audit dashboard."""
    try:
        status = get_audit_status(task_id)
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/audits/<audit_id>/slides-url', methods=['POST'])
@login_required
def save_slides_url(audit_id):
    """Save the generated slides URL to the audit record."""
    try:
        data = request.get_json()
        slides_url = data.get('slides_url')
        if not slides_url:
            return jsonify({'error': 'slides_url required'}), 400
        
        client = supabase_admin or supabase
        client.table('audits').update({
            'slides_url': slides_url
        }).eq('id', audit_id).execute()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================================================
# DEEP AUDIT & PRESENTATION ROUTES / audit-app integration
# =============================================================================

@app.route('/api/project-data/<audit_id>', methods=['GET'])
@login_required
def get_project_data(audit_id):
    """Return full_audit_data from projects table for audit-dashboard.html.
    If no project record exists (old audit), backfill from audits table on-the-fly."""
    try:
        client = supabase_admin or supabase
        result = client.table('projects').select('*').eq('audit_id', audit_id).execute()
        
        if not result.data:
            # ---- BACKFILL: Create project record from existing audit data ----
            logger.info(f"No project found for audit {audit_id}, backfilling from audits table...")
            
            audit_res = client.table('audits').select('*, campaigns(domain)').eq('id', audit_id).single().execute()
            if not audit_res.data:
                return jsonify({'error': 'Audit not found'}), 404
            
            audit = audit_res.data
            audit_results = audit.get('results', {}) or {}
            campaign_data = audit.get('campaigns', {}) or {}
            domain = audit_results.get('competitor_domain') or campaign_data.get('domain', '')
            if domain:
                domain = domain.replace('https://', '').replace('http://', '').rstrip('/')
            
            # Build full_audit_data matching the audit-app format
            import time as time_mod
            full_audit_data = {
                'task_id': audit.get('dataforseo_task_id', ''),
                'domain': domain,
                'status': 'completed' if audit.get('status') == 'completed' else 'pending',
                'created_at': audit.get('created_at', time_mod.strftime("%Y-%m-%dT%H:%M:%SZ")),
                'organic_keywords': audit_results.get('keywords', []),
                'total_keywords': audit_results.get('total_keywords', 0),
                'total_traffic': audit_results.get('total_traffic', 0),
                'keywords_at_limit': audit_results.get('keywords_at_limit', False),
                'backlinks_summary': audit_results.get('backlinks_summary', audit_results.get('backlinks', {})),
                'referring_domains': audit_results.get('referring_domains', []),
                'pages': audit_results.get('pages', []),
                'pagespeed': audit_results.get('pagespeed', {}),
                'max_pages': 200
            }
            
            # If audit data is sparse, try fetching keywords + backlinks from DataForSEO now
            if not full_audit_data['organic_keywords'] and domain:
                try:
                    from api.dataforseo_client import fetch_ranked_keywords, fetch_backlinks_summary, get_referring_domains
                    kw_data = fetch_ranked_keywords(domain)
                    keywords = kw_data.get('keywords', []) if isinstance(kw_data, dict) else []
                    full_audit_data['organic_keywords'] = keywords
                    full_audit_data['total_keywords'] = kw_data.get('total_count', len(keywords))
                    full_audit_data['total_traffic'] = kw_data.get('estimated_traffic', 0)
                    full_audit_data['keywords_at_limit'] = kw_data.get('keywords_at_limit', False)
                    
                    full_audit_data['backlinks_summary'] = fetch_backlinks_summary(domain)
                    full_audit_data['referring_domains'] = get_referring_domains(domain)
                    logger.info(f"Backfill: fetched {len(keywords)} keywords for {domain}")
                except Exception as fetch_err:
                    logger.warning(f"Backfill: could not fetch DataForSEO data: {fetch_err}")
            
            # Save to projects table for future use
            try:
                new_project = client.table('projects').insert({
                    'domain': domain,
                    'full_audit_data': full_audit_data,
                    'source': 'backfill',
                    'audit_id': audit_id
                }).execute()
                project = new_project.data[0]
                logger.info(f"Backfill: created project {project['id']} for audit {audit_id}")
            except Exception as insert_err:
                logger.error(f"Backfill insert failed: {insert_err}")
                # Return data even if insert fails
                return jsonify({
                    'success': True,
                    'project_id': None,
                    'audit_id': audit_id,
                    'domain': domain,
                    'data': full_audit_data
                })
            
            return jsonify({
                'success': True,
                'project_id': project['id'],
                'audit_id': audit_id,
                'domain': domain,
                'data': full_audit_data
            })
            # ---- END BACKFILL ----
        
        project = result.data[0]
        audit_data = project.get('full_audit_data', {}) or {}
        
        # ---- MERGE: Fill missing fields from audits.results ----
        pages = audit_data.get('pages', [])
        pagespeed = audit_data.get('pagespeed')
        needs_update = False
        
        if (not pages or (isinstance(pages, list) and len(pages) == 0)) or not pagespeed:
            try:
                audit_res = client.table('audits').select('results').eq('id', audit_id).execute()
                if audit_res.data:
                    audit_results = audit_res.data[0].get('results', {}) or {}
                    
                    # Merge pages if missing
                    if not pages or (isinstance(pages, list) and len(pages) == 0):
                        src_pages = audit_results.get('pages', [])
                        if isinstance(src_pages, dict):
                            src_pages = src_pages.get('pages', [])
                        if src_pages:
                            audit_data['pages'] = src_pages
                            needs_update = True
                            logger.info(f"Merged {len(src_pages)} pages from audits.results")
                    
                    # Merge pagespeed if missing
                    if not pagespeed:
                        src_ps = audit_results.get('pagespeed')
                        if src_ps:
                            audit_data['pagespeed'] = src_ps
                            needs_update = True
                            logger.info(f"Merged pagespeed from audits.results")
            except Exception as merge_err:
                logger.warning(f"Merge from audits failed: {merge_err}")
        
        # Fetch pagespeed on-the-fly if still missing
        domain = project.get('domain') or audit_data.get('domain', '')
        if not audit_data.get('pagespeed') and domain:
            try:
                from execution.pagespeed_insights import fetch_pagespeed_scores
                ps_data = {}
                mobile = fetch_pagespeed_scores(f"https://{domain}", strategy="mobile")
                if mobile and mobile.get('success'):
                    ps_data['mobile'] = {'scores': mobile.get('scores', {}), 'metrics': mobile.get('metrics', {})}
                    ps_data['scores'] = mobile.get('scores', {})
                    ps_data['metrics'] = mobile.get('metrics', {})
                desktop = fetch_pagespeed_scores(f"https://{domain}", strategy="desktop")
                if desktop and desktop.get('success'):
                    ps_data['desktop'] = {'scores': desktop.get('scores', {}), 'metrics': desktop.get('metrics', {})}
                if ps_data:
                    audit_data['pagespeed'] = ps_data
                    needs_update = True
                    logger.info(f"Fetched pagespeed on-the-fly for {domain}")
            except Exception as ps_err:
                logger.warning(f"On-the-fly pagespeed failed: {ps_err}")
        
        # Persist merged data back to projects table for next time
        if needs_update:
            try:
                client.table('projects').update({
                    'full_audit_data': audit_data
                }).eq('id', project['id']).execute()
                logger.info(f"Persisted merged data back to project {project['id']}")
            except Exception as upd_err:
                logger.warning(f"Could not persist merged data: {upd_err}")
        # ---- END MERGE ----
        
        return jsonify({
            'success': True,
            'project_id': project['id'],
            'audit_id': audit_id,
            'domain': project.get('domain', ''),
            'data': audit_data
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/save-audit-results', methods=['POST'])
@login_required
def save_audit_results():
    """Fetch and save on-page audit results when crawl completes"""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        audit_id = data.get('audit_id')
        task_id = data.get('task_id')
        
        if not audit_id or not task_id:
            return jsonify({"error": "audit_id and task_id required"}), 400
            
        client = supabase_admin or supabase
        
        # Fetch the on-page audit results from DataForSEO
        from api.dataforseo_client import get_page_issues, get_audit_summary
        
        summary_result = get_audit_summary(task_id)
        summary = summary_result.get('summary', {}) if summary_result.get('success') else {}
        
        pages_data = get_page_issues(task_id, limit=200)  # Get up to 200 pages
        pages = pages_data.get('pages', []) if pages_data.get('success') else []
        
        # Get existing audit/project data
        result = client.table('audits').select('*').eq('id', audit_id).execute()
        if not result.data:
            return jsonify({"error": "Audit not found"}), 404
        
        audit_record = result.data[0]
        audit_results = audit_record.get('results', {}) or {}
        
        # Get domain from audit data
        domain = audit_results.get('competitor_domain') or audit_record.get('campaign_id') # Will need to fetch campaign domain if missing
        
        if not domain or str(domain).startswith(('http', 'ww', '1', '2', '3', 'u', 'd', 'e')): # Crude fast check
           try:
              c_res = client.table('campaigns').select('domain').eq('id', audit_record.get('campaign_id')).execute()
              if c_res.data:
                 domain = c_res.data[0]['domain']
           except:
              pass
              
        if domain:
             domain = domain.replace('https://', '').replace('http://', '').rstrip('/')
        
        # Fetch PageSpeed data using Google's PageSpeed Insights API - BOTH mobile and desktop
        pagespeed = {}
        if domain:
            try:
                from execution.pagespeed_insights import fetch_pagespeed_scores
                # Fetch MOBILE
                mobile_result = fetch_pagespeed_scores(f"https://{domain}", strategy="mobile")
                if mobile_result:
                    pagespeed['mobile'] = {
                        'scores': mobile_result.get('scores', {}),
                        'metrics': mobile_result.get('metrics', {})
                    }
                # Fetch DESKTOP
                desktop_result = fetch_pagespeed_scores(f"https://{domain}", strategy="desktop")
                if desktop_result:
                    pagespeed['desktop'] = {
                        'scores': desktop_result.get('scores', {}),
                        'metrics': desktop_result.get('metrics', {})
                    }
                # Also store combined scores for backward compatibility
                if mobile_result:
                    pagespeed['scores'] = mobile_result.get('scores', {})
                    pagespeed['metrics'] = mobile_result.get('metrics', {})
            except Exception as e:
                logger.error(f"PageSpeed error: {e}")
        
        # Update with pages, pagespeed, and mark as completed
        audit_results['summary'] = summary
        categorized = categorize_audit_issues(pages, summary)
        audit_results['categorized'] = categorized
        audit_results['pages'] = pages
        audit_results['pagespeed'] = pagespeed
        
        # Create tasks based on the new audit results (Removed)
        
        # Save back to Supabase
        client.table('audits').update({
            'results': audit_results,
            'status': 'completed'
        }).eq('id', audit_id).execute()
        
        # ---- DUAL WRITE: Also update the projects record for audit-dashboard.html ----
        try:
            project_res = client.table('projects').select('id, full_audit_data').eq('audit_id', audit_id).execute()
            if project_res.data:
                project = project_res.data[0]
                project_data = project.get('full_audit_data', {}) or {}
                project_data['pages'] = pages
                project_data['pagespeed'] = pagespeed
                project_data['status'] = 'completed'
                
                client.table('projects').update({
                    'full_audit_data': project_data
                }).eq('id', project['id']).execute()
                logger.info(f"Dual-write: updated project for audit {audit_id} with pages + pagespeed")
        except Exception as dual_err:
            logger.error(f"Dual-write update to projects failed (non-fatal): {dual_err}")
        # ---- END DUAL WRITE ----
        
        return jsonify({"success": True, "message": "Results saved"})
        
    except Exception as e:
        logger.error(f"Error saving audit results: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/deep-audit/slides', methods=['POST'])
@app.route('/api/deep-audit/generate-slides', methods=['POST'])
@app.route('/api/audits/<audit_id>/generate-slides', methods=['POST'])
@login_required
def generate_deep_audit_slides_endpoint(audit_id=None):
    """Generate modern Google Slides presentation from audit data"""
    try:
        data = request.get_json(silent=True) or {}
            
        screenshots = data.get('screenshots', {})
        audit_data = data.get('audit_data')
        # Use URL parameter if present, otherwise fallback to body
        audit_id = audit_id or data.get('audit_id') or data.get('project_id')  # autoGenerateSlides sends as project_id
        issue_counts = data.get('issue_counts', {})
        template_type = data.get('template_type', '').strip()
        
        client = supabase_admin or supabase
        
        domain = 'unknown'
        
        # If audit_data not provided but audit_id is, fetch from projects table first, then audits
        if not audit_data and audit_id:
            try:
                # Try projects table first (has full_audit_data in the right format)
                proj_res = client.table('projects').select('*').eq('audit_id', audit_id).execute()
                if proj_res.data:
                    project = proj_res.data[0]
                    audit_data = project.get('full_audit_data', {})
                    domain = project.get('domain', audit_data.get('domain', 'unknown'))
                    logger.info(f"Slides: loaded data from projects table for audit {audit_id}")
                    
                    # Merge missing fields from audits.results (readability, pagespeed, pages)
                    needs_merge = (
                        not audit_data.get('readability_results') or
                        not audit_data.get('pagespeed') or
                        not audit_data.get('pages')
                    )
                    if needs_merge:
                        try:
                            audit_row = client.table('audits').select('results').eq('id', audit_id).execute()
                            if audit_row.data:
                                ar = audit_row.data[0].get('results', {}) or {}
                                if not audit_data.get('readability_results') and ar.get('readability_results'):
                                    audit_data['readability_results'] = ar['readability_results']
                                    logger.info("Slides: merged readability_results from audits")
                                if not audit_data.get('pagespeed') and ar.get('pagespeed'):
                                    audit_data['pagespeed'] = ar['pagespeed']
                                    logger.info("Slides: merged pagespeed from audits")
                                if not audit_data.get('pages') and ar.get('pages'):
                                    audit_data['pages'] = ar['pages']
                                    logger.info("Slides: merged pages from audits")
                        except Exception as merge_err:
                            logger.warning(f"Slides: merge from audits failed: {merge_err}")
                else:
                    # Fallback to audits table
                    result = client.table('audits').select('*, campaigns(domain)').eq('id', audit_id).execute()
                    if result.data:
                        record = result.data[0]
                        results_dict = record.get('results', {}) or {}
                        campaign_data = record.get('campaigns', {}) or {}
                        
                        domain = results_dict.get('competitor_domain') or campaign_data.get('domain', 'unknown')
                        
                        audit_data = {
                            **results_dict,
                            'domain': domain,
                            'pages': results_dict.get('pages', []),
                            'pagespeed': results_dict.get('pagespeed', {}),
                            'organic_keywords': results_dict.get('keywords', []),
                            'backlinks_summary': results_dict.get('backlinks_summary', results_dict.get('backlinks', {})),
                            'referring_domains': results_dict.get('referring_domains', [])
                        }
                        logger.info(f"Slides: loaded data from audits table for audit {audit_id}")
            except Exception as e:
                logger.error(f"Error fetching project for slides: {e}")

        if not audit_data:
            # Social media slides only need screenshots, not audit_data
            if template_type == 'social_media':
                audit_data = {'domain': domain or 'unknown'}
            else:
                return jsonify({"error": "No audit data provided and could not fetch from audit_id"}), 400

        # Ensure critical nested fields are dictionaries if they are strings
        for field in ['domain_rank', 'summary', 'backlinks_summary', 'organic_keywords', 'pages', 'referring_domains']:
            if isinstance(audit_data.get(field), str):
                import json
                try:
                    audit_data[field] = json.loads(audit_data[field])
                except:
                    pass
        
        if not domain or domain == 'unknown':
           domain = audit_data.get('domain', 'Website')
           
        logger.info(f"Generating slides for {domain}")
        
        # Get Google credentials
        creds = get_google_credentials()
        if not creds:
            return jsonify({"error": "Google credentials not available"}), 500
        
        # Upload screenshots to Supabase Storage if present
        processed_screenshots = {}
        try:
            if not isinstance(screenshots, dict):
                screenshots = {}

            # Fallback for Homepage
            try:
                hp = screenshots.get('homepage')
                is_homepage_missing = not hp or len(str(hp)) < 100
                if is_homepage_missing and domain and domain != 'unknown':
                    homepage_b64 = capture_screenshot_with_fallback(domain)
                    if homepage_b64:
                        screenshots['homepage'] = homepage_b64
            except Exception as e:
                logger.error(f"Homepage fallback error: {e}")

            if screenshots:
                import base64
                import uuid
                
                bucket_name = 'audit-screenshots'
                try:
                    buckets = client.storage.list_buckets()
                    existing_buckets = [b.name for b in buckets]
                    if bucket_name not in existing_buckets:
                        client.storage.create_bucket(bucket_name, options={"public": True})
                except Exception as e:
                    pass

                for key, data_uri in screenshots.items():
                    try:
                        if not data_uri or not isinstance(data_uri, str): continue
                        if data_uri.startswith('http'):
                            processed_screenshots[key] = data_uri
                            continue
                            
                        import mimetypes
                        
                        mime_type = "image/png"
                        ext = ".png"
                        if data_uri.startswith('data:'):
                            mime_type = data_uri.split(';')[0].split(':')[1]
                            ext = mimetypes.guess_extension(mime_type) or ".png"

                        if ',' in data_uri:
                            _, encoded = data_uri.split(',', 1)
                        else:
                            encoded = data_uri
                            
                        img_data = base64.b64decode(encoded)
                        
                        # Fix: Google Slides API rejects WebP images. If it's WebP, convert to PNG.
                        if "webp" in mime_type.lower():
                            try:
                                import io
                                from PIL import Image
                                img = Image.open(io.BytesIO(img_data))
                                png_bio = io.BytesIO()
                                img.save(png_bio, format="PNG")
                                img_data = png_bio.getvalue()
                                mime_type = "image/png"
                                ext = ".png"
                                logger.info(f"Converted WebP screenshot '{key}' to PNG")
                            except Exception as conv_err:
                                logger.error(f"Failed to convert WebP to PNG: {conv_err}")

                        logger.info(f"Screenshot '{key}': {len(img_data)} bytes ({len(img_data)/1024/1024:.1f} MB) [{mime_type}]")
                        filename = f"{uuid.uuid4()}{ext}"
                        
                        client.storage.from_(bucket_name).upload(
                            file=img_data,
                            path=filename,
                            file_options={"content-type": mime_type, "x-upsert": "true"}
                        )
                        
                        public_url = client.storage.from_(bucket_name).get_public_url(filename)
                        logger.info(f"Screenshot '{key}' uploaded -> {public_url[:100]}")
                        processed_screenshots[key] = public_url
                        
                    except Exception as e:
                        logger.error(f"Screenshot '{key}' upload failed: {e}")
                        continue
                        
        except Exception as e:
            processed_screenshots = {} 

        issue_counts = data.get('issue_counts', None)

        # Fetch template_type from DB if not sent in body (SEO flow)
        if not template_type:
            try:
                template_res = client.table('audits').select('results, campaign_id').eq('id', audit_id).execute()
                if template_res.data:
                    ar = template_res.data[0].get('results', {}) or {}
                    template_type = ar.get('template_type', '')
                    
                    # If no explicit template_type in audit, map from client's focus field
                    if not template_type:
                        campaign_id = template_res.data[0].get('campaign_id')
                        if campaign_id:
                            try:
                                camp_res = client.table('campaigns').select('settings').eq('id', campaign_id).execute()
                                if camp_res.data:
                                    settings = camp_res.data[0].get('settings', {}) or {}
                                    focus = (settings.get('focus') or '').strip()
                                    focus_to_template = {
                                        'Product': 'ecommerce',
                                        'product': 'ecommerce',
                                        'ecommerce': 'ecommerce',
                                        'E-commerce': 'ecommerce',
                                        'Service': 'local',
                                        'service': 'local',
                                        'Local': 'local',
                                        'local': 'local',
                                        'SaaS': 'default',
                                        'saas': 'default',
                                        'Production': 'production',
                                        'production': 'production',
                                    }
                                    template_type = focus_to_template.get(focus, 'local')
                                    print(f"DEBUG: Mapped client focus '{focus}' -> template_type '{template_type}'", file=sys.stderr)
                            except Exception as e:
                                print(f"DEBUG: Could not fetch campaign focus: {e}", file=sys.stderr)
            except: pass
        
        if not template_type:
            template_type = 'local'
        
        print(f"DEBUG: Final template_type = '{template_type}'", file=sys.stderr)

        # Generate presentation using create_deep_audit_slides
        result = create_deep_audit_slides(
            data=audit_data,
            domain=domain,
            creds=creds,
            screenshots=processed_screenshots,
            issue_counts=issue_counts,
            template_type=template_type
        )
        
        if result and result.get('presentation_id'):
            # Save the slide URL back to the audit table so it appears in the agency UI
            if audit_id:
                try:
                    res = client.table('audits').select('results').eq('id', audit_id).execute()
                    if res.data:
                       ar = res.data[0].get('results', {}) or {}
                       ar['presentation_url'] = result.get('presentation_url')
                       client.table('audits').update({'results': ar}).eq('id', audit_id).execute()
                except: pass
        
            return jsonify({
                "success": True,
                "presentation_id": result.get('presentation_id'),
                "presentation_url": result.get('presentation_url')
            })
        else:
            return jsonify({"error": result.get('error', 'Failed to generate slides')}), 500
        
    except Exception as e:
        logger.error(f"Error generating deep slides: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Internal Error: {str(e)}"}), 500

@app.route('/api/audit/<audit_id>/readability', methods=['GET'])
@login_required
def analyze_readability(audit_id):
    """Analyze content readability for audit pages"""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        client = supabase_admin or supabase
        result = client.table('audits').select('*').eq('id', audit_id).execute()
        
        if not result.data:
            return jsonify({"success": False, "error": "Audit not found"}), 404
            
        audit_record = result.data[0]
        audit_data = audit_record.get('results', {}) or {}

        if audit_data.get('readability_results') and not request.args.get('refresh'):
            return jsonify({"success": True, "results": audit_data.get('readability_results')})
            
        pages = audit_data.get('pages', [])
        candidates = []
        
        def is_homepage(u):
            from urllib.parse import urlparse
            parsed = urlparse(u)
            path = parsed.path.strip('/')
            return path == '' or path in ['index.html', 'index.php', 'home']
            
        blacklist = ['/collections', '/cart', '/checkout', '/account', '/search', '/policies/', '/pages/', '/collections/']
        blog_keywords = ['/blog', '/blogs', '/article', '/post', '/news', '/insight', '/guide', '202']
        product_keywords = ['/products/', '/product/', '/p/', '/item/']
            
        for page in pages:
            url = page.get('url', '')
            traffic = page.get('traffic', 0)
            if is_homepage(url): continue
            if any(item in url.lower() for item in blacklist): continue
            is_blog = any(keyword in url.lower() for keyword in blog_keywords)
            is_product = any(keyword in url.lower() for keyword in product_keywords)
            candidates.append({'url': url, 'traffic': traffic, 'is_blog': is_blog, 'is_product': is_product})
            
        candidates.sort(key=lambda x: (x['is_product'], x['is_blog'], x['traffic']), reverse=True)
        top_candidates = [c['url'] for c in candidates[:3]]
        
        if len(top_candidates) < 3:
            urls = [p.get('url') for p in pages if p.get('url')]
            
            # Simple heuristic sort: prefer paths with more slashes or hyphens which indicate content
            def sort_score(url):
                if is_homepage(url): return -10
                if any(k in url.lower() for k in blacklist): return -5
                if any(k in url.lower() for k in product_keywords): return 5
                return url.count('-') + url.count('/')
                
            sorted_urls = sorted(urls, key=sort_score, reverse=True)
            for u in sorted_urls:
                if u not in top_candidates:
                    top_candidates.append(u)
                if len(top_candidates) >= 3:
                    break
            
        if not top_candidates:
            return jsonify({"success": False, "error": "No suitable pages found for readability analysis"})
            
        from execution.readability import mass_analyze_urls
        readability_results = mass_analyze_urls(top_candidates)
        
        if readability_results:
            audit_data['readability_results'] = readability_results
            client.table('audits').update({'results': audit_data}).eq('id', audit_id).execute()
            
            return jsonify({"success": True, "results": readability_results})
        else:
            return jsonify({"success": False, "error": "Analysis failed for all candidates"})
            
    except Exception as e:
        logger.error(f"Error in readability API: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/pagespeed', methods=['POST'])
@login_required
def check_pagespeed():
    """Check PageSpeed Insights for a given URL"""
    try:
        data = request.json
        url = data.get('url')
        strategy = data.get('strategy', 'mobile')
        
        if not url:
            return jsonify({'error': 'URL is required'}), 400
            
        # Add http if missing to prevent API errors
        if not url.startswith('http'):
            url = f'https://{url}'
            
        from execution.pagespeed_insights import fetch_pagespeed_scores
        results = fetch_pagespeed_scores(url, strategy)
        
        if results:
            return jsonify(results)
        else:
            return jsonify({'error': 'Failed to fetch PageSpeed data'}), 500
            
    except Exception as e:
        logger.error(f"Error checking pagespeed: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# PAGESPEED REFRESH
# =============================================================================

@app.route('/api/audit/<audit_id>/refresh-speed', methods=['POST'])
@login_required
def refresh_pagespeed(audit_id):
    """Fetch fresh PageSpeed data for an audit's domain and save it."""
    try:
        client = supabase_admin or supabase
        
        # Get domain from audit
        audit_res = client.table('audits').select('results, campaign_id, campaigns(domain)').eq('id', audit_id).execute()
        if not audit_res.data:
            return jsonify({'error': 'Audit not found'}), 404
        
        record = audit_res.data[0]
        results = record.get('results', {}) or {}
        campaign = record.get('campaigns', {}) or {}
        domain = results.get('competitor_domain') or campaign.get('domain', '')
        if domain:
            domain = domain.replace('https://', '').replace('http://', '').rstrip('/')
        
        if not domain:
            return jsonify({'error': 'No domain found'}), 400
        
        from execution.pagespeed_insights import fetch_pagespeed_scores
        pagespeed = {}
        
        mobile = fetch_pagespeed_scores(f"https://{domain}", strategy="mobile")
        if mobile and mobile.get('success'):
            pagespeed['mobile'] = {'scores': mobile.get('scores', {}), 'metrics': mobile.get('metrics', {})}
            pagespeed['scores'] = mobile.get('scores', {})
            pagespeed['metrics'] = mobile.get('metrics', {})
        
        # Desktop fetch skipped during auto-generate to save ~80s
        # Users can manually trigger desktop via the dashboard's refresh button
        
        if not pagespeed:
            return jsonify({'error': 'PageSpeed fetch failed'}), 500
        
        # Save to audits.results
        results['pagespeed'] = pagespeed
        client.table('audits').update({'results': results}).eq('id', audit_id).execute()
        
        # Save to projects.full_audit_data
        try:
            proj = client.table('projects').select('id, full_audit_data').eq('audit_id', audit_id).execute()
            if proj.data:
                fad = proj.data[0].get('full_audit_data', {}) or {}
                fad['pagespeed'] = pagespeed
                client.table('projects').update({'full_audit_data': fad}).eq('id', proj.data[0]['id']).execute()
        except Exception as e:
            logger.warning(f"Could not update projects pagespeed: {e}")
        
        logger.info(f"PageSpeed refreshed for {domain}: perf={pagespeed.get('scores', {}).get('performance', 'N/A')}")
        return jsonify({'success': True, 'pagespeed': pagespeed})
    
    except Exception as e:
        logger.error(f"Error refreshing pagespeed: {e}")
        return jsonify({'error': str(e)}), 500


# ==========================================================================
# SOCIAL MEDIA AUDIT ENDPOINTS
# ==========================================================================

# In-memory store for social audit progress
_social_audit_jobs = {}


def _run_social_audit_pipeline(job_id, username, niche="", location="", recency_days=180):
    """Background pipeline: scrape → transcribe → extract hooks → store."""
    job = _social_audit_jobs[job_id]
    try:
        # Step 1: Baseline Profile
        job["status"] = "scraping_baseline"
        job["progress"] = 10
        profile = scrape_instagram_profile(username)
        if not profile:
            job["status"] = "failed"
            job["error"] = f"Could not scrape profile @{username}"
            return
        job["data"]["profile"] = profile
        job["progress"] = 15

        # Step 1.5: Screenshot the client's Instagram profile page
        job["status"] = "screenshotting_profile"
        try:
            from execution.instagram_scraper import screenshot_instagram_profile
            ig_screenshot_url = screenshot_instagram_profile(username)
            if ig_screenshot_url:
                job["data"]["ig_profile_screenshot"] = ig_screenshot_url
                logger.info(f"Instagram profile screenshot captured for @{username}")
            else:
                logger.warning(f"Could not capture Instagram profile screenshot for @{username}")
        except Exception as e:
            logger.error(f"Profile screenshot step failed: {e}")
        job["progress"] = 18

        # Step 2: Discover Top Competitors & Fetch Viral Outlier Reels
        job["status"] = "discovering_competitors"
        job["progress"] = 20
        comp_keyword = niche or profile.get("category", "") or username
        
        from execution.instagram_scraper import get_top_competitors_best_reels
        
        reels = []
        try:
            reels = get_top_competitors_best_reels(
                niche_keyword=comp_keyword,
                location=location,
                limit=7,
                top_reels=15,
                recency_days=recency_days
            )
        except Exception as e:
            logger.error(f"Competitor outlier extraction failed: {e}")
            
        job["data"]["reels_raw"] = len(reels)
        job["progress"] = 40

        # Step 3: Transcribe Viral Reels
        job["status"] = "transcribing"
        job["progress"] = 45
        if reels:
            reels = batch_transcribe_reels(reels, language="en")
        job["progress"] = 65

        # Step 4: Extract hooks & Actionable Strategy
        job["status"] = "extracting_hooks"
        job["progress"] = 70
        
        ai_strategy = []
        if reels:
            reels = extract_hooks_batch(reels)
            
            from execution.hook_extractor import generate_actionable_strategy
            job["status"] = "generating_strategy"
            job["progress"] = 80
            
            try:
                ai_strategy = generate_actionable_strategy(profile, reels)
            except Exception as e:
                logger.error(f"Strategy generation failed: {e}")
            
        job["data"]["reels"] = reels
        job["data"]["ai_strategy"] = ai_strategy
        job["progress"] = 90

        # Step 5: Discover competitors
        # Step 5: Save to Supabase if available
        job["status"] = "saving"
        try:
            if supabase:
                clean_reels = [{k: v for k, v in r.items() if k != 'raw'} for r in reels]
                clean_profile = {k: v for k, v in profile.items() if k != 'raw'}

                audit_data = {
                    "id": job_id,
                    "type": "social_media",
                    "status": "completed",
                    "results": {
                        "profile": clean_profile,
                        "reels": clean_reels, # These are now the competitor OUTLIER reels
                        "ai_strategy": ai_strategy,
                        "ig_profile_screenshot": job["data"].get("ig_profile_screenshot"),
                        "stats": {
                            "total_reels": len(reels),
                            "transcribed": sum(1 for r in reels if r.get("transcript")),
                            "hooks_extracted": sum(1 for r in reels if r.get("hook_text"))
                        }
                    }
                }
                supabase.table('audits').upsert(audit_data).execute()
                logger.info(f"Social audit {job_id} saved to Supabase")
        except Exception as e:
            logger.error(f"Failed to save social audit to Supabase: {e}")

        job["status"] = "completed"
        job["progress"] = 100
        logger.info(f"Social audit {job_id} completed: {len(reels)} viral competitor reels processed")

    except Exception as e:
        logger.error(f"Social audit pipeline failed: {e}")
        job["status"] = "failed"
        job["error"] = str(e)


def _run_social_audit_pipeline_from_prospect(prospect_id):
    """Background pipeline: use pre-found competitors → transcribe → extract hooks → store."""
    client = supabase_admin or supabase
    try:
        logger.info(f"Starting audit pipeline from prospect {prospect_id}")
        # Fetch prospect
        res = client.table('social_prospects').select('*').eq('id', prospect_id).single().execute()
        if not res.data:
            logger.error(f"Prospect {prospect_id} not found")
            return
        p = res.data
        username = p['username'].replace('@', '')
        competitors = p.get('competitors_data') or []
        
        # We process this audit under the prospect_id as the job_id
        job_id = prospect_id 
        
        # Step 1: Baseline Profile
        client.table('social_prospects').update({'status': 'analyzing'}).eq('id', prospect_id).execute()
        
        # Import dynamically if not at top-level
        from execution.instagram_scraper import scrape_instagram_profile, screenshot_instagram_profile
        
        logger.info(f"Scraping profile for @{username}")
        profile = scrape_instagram_profile(username)
        if not profile:
            client.table('social_prospects').update({'status': 'analysis_error'}).eq('id', prospect_id).execute()
            return
            
        ig_screenshot_url = None
        try:
            ig_screenshot_url = screenshot_instagram_profile(username)
            if ig_screenshot_url:
                logger.info(f"Instagram profile screenshot captured for @{username}")
        except Exception as e:
            logger.error(f"Profile screenshot step failed: {e}")

        # Step 2: Extract reels from PRE-FOUND competitors
        logger.info(f"Extracting best reels from {len(competitors)} pre-found competitors...")
        from execution.instagram_scraper import get_best_reels_from_competitor_list
        reels = get_best_reels_from_competitor_list(competitors, limit=6, top_reels=15, recency_days=180)
        
        # Step 3: Transcribe
        if reels:
            logger.info("Transcribing reels...")
            reels = batch_transcribe_reels(reels, language="en")
            
        # Step 4: Extract hooks & Strategy
        ai_strategy = []
        if reels:
            logger.info("Extracting hooks...")
            reels = extract_hooks_batch(reels)
            from execution.hook_extractor import generate_actionable_strategy
            try:
                logger.info("Generating actionable strategy...")
                ai_strategy = generate_actionable_strategy(profile, reels)
            except Exception as e:
                logger.error(f"Strategy generation failed: {e}")
                
        # Step 5: Save to audits table so it appears in standard audit dashboard
        logger.info("Saving audit to database...")
        clean_reels = [{k: v for k, v in r.items() if k != 'raw'} for r in reels]
        clean_profile = {k: v for k, v in profile.items() if k != 'raw'}
        
        audit_data = {
            "id": job_id, 
            "type": "social_media",
            "status": "completed",
            "results": {
                "profile": clean_profile,
                "reels": clean_reels,
                "ai_strategy": ai_strategy,
                "ig_profile_screenshot": ig_screenshot_url,
                "stats": {
                    "total_reels": len(reels),
                    "transcribed": sum(1 for r in reels if r.get("transcript")),
                    "hooks_extracted": sum(1 for r in reels if r.get("hook_text"))
                }
            }
        }
        client.table('audits').upsert(audit_data).execute()
        
        # Update the prospect so the UI knows it's done
        # We can link the analysis_data to {"audit_id": job_id}
        client.table('social_prospects').update({
            'status': 'analyzed',
            'analysis_data': {"audit_id": job_id} 
        }).eq('id', prospect_id).execute()
        
        logger.info(f"Analysis complete for @{username} based on predefined competitors!")

    except Exception as e:
        logger.error(f"Analysis pipeline failed for prospect {prospect_id}: {e}")
        import traceback
        traceback.print_exc()
        client.table('social_prospects').update({'status': 'analysis_error'}).eq('id', prospect_id).execute()


@app.route('/api/social-audit/start', methods=['POST'])
def start_social_audit():
    """Start a social media audit for an Instagram username."""
    data = request.json or {}
    username = data.get('username', '').strip().lstrip('@')
    niche = data.get('niche', '')
    location = data.get('location', '')
    recency_days = int(data.get('recency_days', 180))

    if not username:
        return jsonify({'error': 'Instagram username is required'}), 400

    job_id = str(uuid.uuid4())
    _social_audit_jobs[job_id] = {
        "id": job_id,
        "username": username,
        "status": "starting",
        "progress": 0,
        "data": {},
        "error": None
    }

    thread = threading.Thread(
        target=_run_social_audit_pipeline,
        args=(job_id, username, niche, location, recency_days),
        daemon=True
    )
    thread.start()

    return jsonify({
        'success': True,
        'audit_id': job_id,
        'username': username,
        'message': f'Social media audit started for @{username}'
    })


@app.route('/api/social-audit/<audit_id>/status', methods=['GET'])
def social_audit_status(audit_id):
    """Check the status of a social media audit."""
    job = _social_audit_jobs.get(audit_id)
    if not job:
        if supabase:
            try:
                res = supabase.table('audits').select('id,status,results').eq('id', audit_id).eq('type', 'social_media').execute()
                if res.data:
                    return jsonify({
                        'status': res.data[0].get('status', 'completed'),
                        'progress': 100,
                        'has_data': bool(res.data[0].get('results'))
                    })
            except:
                pass
        return jsonify({'error': 'Audit not found'}), 404

    return jsonify({
        'status': job['status'],
        'progress': job['progress'],
        'username': job.get('username', ''),
        'error': job.get('error'),
        'has_data': bool(job.get('data'))
    })


@app.route('/api/social-audit/<audit_id>/data', methods=['GET'])
def social_audit_data(audit_id):
    """Get the full data for a social media audit."""
    job = _social_audit_jobs.get(audit_id)

    if job and job.get('data'):
        data = job['data'].copy()
        if 'profile' in data and 'raw' in data['profile']:
            data['profile'] = {k: v for k, v in data['profile'].items() if k != 'raw'}
        if 'reels' in data:
            data['reels'] = [{k: v for k, v in r.items() if k != 'raw'} for r in data['reels']]
        if 'competitors' in data:
            data['competitors'] = [{k: v for k, v in c.items() if k != 'raw'} for c in data['competitors']]
        return jsonify({'success': True, 'data': data})

    if supabase:
        try:
            res = supabase.table('audits').select('results').eq('id', audit_id).execute()
            if res.data and res.data[0].get('results'):
                return jsonify({'success': True, 'data': res.data[0]['results']})
        except:
            pass

    return jsonify({'error': 'No data available yet'}), 404


# In-memory store for influencer discovery jobs
_influencer_discovery_jobs = {}


@app.route('/api/influencers/discover', methods=['POST'])
@login_required
def discover_influencers():
    """Start influencer discovery in background thread. Returns job_id for polling."""
    data = request.json or {}
    niche = data.get('niche', '').strip()
    location = data.get('location', '').strip()
    
    try:
        min_followers = int(data.get('min_followers', 10000))
        max_followers = int(data.get('max_followers', 100000))
        limit = int(data.get('limit', 20))
    except ValueError:
        return jsonify({'error': 'Follower values and limit must be numbers'}), 400

    if not niche:
        return jsonify({'error': 'Niche keyword is required'}), 400

    job_id = str(uuid.uuid4())
    _influencer_discovery_jobs[job_id] = {
        'status': 'searching',
        'influencers': [],
        'error': None
    }

    def _run_discovery():
        try:
            from execution.instagram_scraper import find_influencers_by_niche
            
            influencers = find_influencers_by_niche(
                niche_keyword=niche,
                location=location,
                min_followers=min_followers,
                max_followers=max_followers,
                limit=limit
            )
            
            _influencer_discovery_jobs[job_id]['influencers'] = influencers
            _influencer_discovery_jobs[job_id]['status'] = 'done'
            logger.info(f"Discovery job {job_id}: found {len(influencers)} influencers")
            
        except Exception as e:
            logger.error(f"Discovery job {job_id} failed: {e}")
            _influencer_discovery_jobs[job_id]['status'] = 'error'
            _influencer_discovery_jobs[job_id]['error'] = str(e)

    threading.Thread(target=_run_discovery, daemon=True).start()
    
    return jsonify({
        'success': True,
        'job_id': job_id,
        'message': 'Influencer discovery started'
    })


@app.route('/api/influencers/discover/<job_id>/status', methods=['GET'])
@login_required
def discover_influencers_status(job_id):
    """Poll for influencer discovery results."""
    job = _influencer_discovery_jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    result = {
        'status': job['status'],
        'error': job.get('error')
    }
    
    if job['status'] == 'done':
        result['influencers'] = job['influencers']
        result['count'] = len(job['influencers'])
        result['success'] = True
        # Clean up after delivery
        # (keep it for a few minutes in case of re-polls)
    
    return jsonify(result)


@app.route('/api/influencers/save', methods=['POST'])
@login_required
def save_influencers():
    """Save discovered influencers to the social_prospects table."""
    data = request.json or {}
    influencers = data.get('influencers', [])
    niche = data.get('niche', '')
    
    if not influencers:
        return jsonify({'error': 'No influencers to save'}), 400
    
    client = supabase_admin or supabase
    saved = 0
    skipped = 0
    
    for inf in influencers:
        # Check for existing by username to avoid dupes
        existing = client.table('social_prospects').select('id').eq('username', inf.get('username', '')).execute()
        if existing.data:
            skipped += 1
            continue
            
        row = {
            'username': inf.get('username', ''),
            'full_name': inf.get('full_name', ''),
            'followers': inf.get('followers', 0),
            'following': inf.get('following', 0),
            'bio': inf.get('bio', ''),
            'profile_pic_url': inf.get('profile_pic_url', ''),
            'engagement_rate': inf.get('engagement_rate', 0),
            'is_verified': inf.get('is_verified', False),
            'category': inf.get('category', ''),
            'external_url': inf.get('external_url', ''),
            'niche': niche,
            'status': 'new'
        }
        
        client.table('social_prospects').insert(row).execute()
        saved += 1
    
    return jsonify({'success': True, 'saved': saved, 'skipped': skipped})


@app.route('/api/influencers/list', methods=['GET'])
@login_required
def list_influencers():
    """List all saved influencer prospects."""
    client = supabase_admin or supabase
    
    try:
        res = client.table('social_prospects').select('*').order('created_at', desc=True).execute()
        return jsonify({'success': True, 'prospects': res.data or []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/influencers/<prospect_id>', methods=['GET'])
@login_required
def get_influencer(prospect_id):
    """Get a single prospect with full data."""
    client = supabase_admin or supabase
    
    try:
        res = client.table('social_prospects').select('*').eq('id', prospect_id).single().execute()
        return jsonify({'success': True, 'prospect': res.data})
    except Exception as e:
        return jsonify({'error': str(e)}), 404


@app.route('/api/influencers/<prospect_id>/competitors', methods=['POST'])
@login_required
def generate_prospect_competitors(prospect_id):
    """Generate 4-5 competitors for a prospect."""
    import threading
    
    data = request.json or {}
    comp_niche = data.get('niche', '')
    min_followers = int(data.get('min_followers', 50000))
    max_followers = int(data.get('max_followers', 500000))
    limit = min(int(data.get('limit', 5)), 5)  # Cap at 5
    
    client = supabase_admin or supabase
    
    # Fetch the prospect
    prospect = client.table('social_prospects').select('*').eq('id', prospect_id).single().execute()
    if not prospect.data:
        return jsonify({'error': 'Prospect not found'}), 404
    
    p = prospect.data
    niche_keyword = comp_niche or p.get('niche', '') or p.get('category', '')
    
    if not niche_keyword:
        return jsonify({'error': 'No niche keyword provided or on prospect'}), 400
    
    # Update status
    client.table('social_prospects').update({'status': 'finding_competitors'}).eq('id', prospect_id).execute()
    
    def _run_competitor_search():
        try:
            from execution.instagram_scraper import discover_competitors
            
            competitors = discover_competitors(
                niche_keyword=niche_keyword,
                min_followers=min_followers,
                max_followers=max_followers,
                limit=limit
            )
            
            # Clean competitors (remove raw data to save space)
            clean_comps = []
            for c in competitors:
                clean_comps.append({
                    'username': c.get('username', ''),
                    'full_name': c.get('full_name', ''),
                    'followers': c.get('followers', 0),
                    'profile_pic_url': c.get('profile_pic_url', ''),
                    'engagement_rate': c.get('engagement_rate', 0),
                    'is_verified': c.get('is_verified', False),
                    'category': c.get('category', ''),
                })
            
            client.table('social_prospects').update({
                'competitors_data': clean_comps,
                'status': 'competitors_found'
            }).eq('id', prospect_id).execute()
            
            logger.info(f"Found {len(clean_comps)} competitors for @{p.get('username')}")
            
        except Exception as e:
            logger.error(f"Competitor search failed for {prospect_id}: {e}")
            client.table('social_prospects').update({'status': 'competitor_error'}).eq('id', prospect_id).execute()
    
    threading.Thread(target=_run_competitor_search, daemon=True).start()
    return jsonify({'success': True, 'message': 'Competitor search started'})


@app.route('/api/influencers/<prospect_id>/analyze', methods=['POST'])
@login_required
def analyze_prospect(prospect_id):
    """Run full social media audit using pre-found competitors."""
    import threading
    
    # We just kick off the new pipeline thread
    threading.Thread(target=_run_social_audit_pipeline_from_prospect, args=(prospect_id,), daemon=True).start()
    
    return jsonify({'success': True, 'message': 'Analysis started using pre-found competitors'})


@app.route('/api/influencers/<prospect_id>', methods=['DELETE'])
@login_required
def delete_influencer(prospect_id):
    """Delete a prospect."""
    client = supabase_admin or supabase
    try:
        client.table('social_prospects').delete().eq('id', prospect_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("Starting server...")
    port = int(os.environ.get('PORT', 5002)) # Default to 5002 as requested
    print(f"Running on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True)
