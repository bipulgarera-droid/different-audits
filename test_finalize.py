from supabase import create_client
import sys

URL = "https://vmkjbmthsjlyhdwtghks.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZta2pibXRoc2pseWhkd3RnaGtzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjI5NTMwNCwiZXhwIjoyMDg3ODcxMzA0fQ.EjnVoEcg2jekLQGduuS45d9-FidEzC4SoT0PEBYw64A"

try:
    print("Connecting to Supabase...")
    supabase = create_client(URL, KEY)
    
    # Get all crawling audits
    response = supabase.table('audits').select('*').eq('status', 'crawling').execute()
    audits = response.data
    
    print(f"Found {len(audits)} crawling audits.")
    
    for a in audits:
        print(f"Audit ID: {a['id']}, Task ID: {a.get('dataforseo_task_id')}")
        
except Exception as e:
    print("Error:", e)
