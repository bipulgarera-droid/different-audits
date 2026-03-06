import re
import requests

with open('.env', 'r') as f:
    text = f.read()

url_match = re.search(r'SUPABASE_URL=(.+)', text)
key_match = re.search(r'SUPABASE_SERVICE_ROLE_KEY=(.+)', text)

if url_match and key_match:
    url = url_match.group(1).strip()
    key = key_match.group(1).strip()
    
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    
    res = requests.get(f"{url}/rest/v1/campaigns?select=id&limit=1", headers=headers)
    print("campaigns:", res.status_code, res.text)
    
    res = requests.get(f"{url}/rest/v1/audits?select=id&limit=1", headers=headers)
    print("audits:", res.status_code, res.text)
    
    res = requests.get(f"{url}/rest/v1/projects?select=id&limit=1", headers=headers)
    print("projects:", res.status_code, res.text)
else:
    print("Could not find keys in .env")
