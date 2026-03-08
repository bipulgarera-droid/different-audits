#!/usr/bin/env python3
"""
One-time migration: create the social_prospects table in Supabase.
Run with: python execution/create_social_prospects_table.py
"""
import os
import sys
from dotenv import load_dotenv

# Load env from the project root
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

from supabase import create_client

url = os.environ.get('SUPABASE_URL', '')
key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '') or os.environ.get('SUPABASE_KEY', '')

if not url or not key:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    sys.exit(1)

sb = create_client(url, key)

SQL = """
CREATE TABLE IF NOT EXISTS social_prospects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username TEXT NOT NULL,
    full_name TEXT DEFAULT '',
    followers INT DEFAULT 0,
    following INT DEFAULT 0,
    bio TEXT DEFAULT '',
    profile_pic_url TEXT DEFAULT '',
    engagement_rate FLOAT DEFAULT 0,
    is_verified BOOLEAN DEFAULT FALSE,
    category TEXT DEFAULT '',
    external_url TEXT DEFAULT '',
    niche TEXT DEFAULT '',
    status TEXT DEFAULT 'new',
    analysis_data JSONB DEFAULT '{}'::jsonb,
    competitors_data JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_social_prospects_created_at ON social_prospects (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_social_prospects_niche ON social_prospects (niche);
"""

print("Creating social_prospects table...")
try:
    sb.postgrest.rpc('', {}).execute()  # dummy to test connection
except:
    pass

# Use the SQL editor via REST
import requests
headers = {
    "apikey": key,
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json"
}

# Execute via Supabase SQL endpoint
resp = requests.post(
    f"{url}/rest/v1/rpc/",
    headers=headers,
    json={}
)

# Alternative: just use psycopg2 or the supabase client
# Since we can't run raw SQL easily via the REST API,
# let's just try to create the table by inserting and catching errors
# or we can use the supabase-py client's table method

# Actually, the simplest approach: just try to use the table.
# If it doesn't exist, we print the SQL for the user to run in the Supabase dashboard.
print("\n" + "="*60)
print("PLEASE RUN THE FOLLOWING SQL IN YOUR SUPABASE SQL EDITOR:")
print("="*60)
print(SQL)
print("="*60)
print("\nGo to: https://supabase.com/dashboard → SQL Editor → New Query")
print("Paste the SQL above and click 'Run'")
print("\nAfter that, the social_prospects table will be ready to use.")
