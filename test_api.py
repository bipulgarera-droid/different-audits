import requests

# Try to fetch campaigns
res = requests.get('http://127.0.0.1:5001/api/campaigns')
print("GET /api/campaigns ->", res.status_code, res.text)

if res.status_code == 200:
    data = res.json()
    campaigns = data.get('campaigns', [])
    if campaigns:
        cid = campaigns[0]['id']
        payload = {
            "campaign_id": cid,
            "max_pages": 10,
            "template_type": "local",
            "type": "technical"
        }
        print("POST /api/audits with payload:", payload)
        res2 = requests.post('http://127.0.0.1:5001/api/audits', json=payload)
        print("POST /api/audits ->", res2.status_code, res2.text)
    else:
        print("No campaigns exist. Cannot test audit creation without a campaign.")
        
        # Try to create a campaign
        new_camp = {"name": "Test Client", "domain": "example.com"}
        r = requests.post('http://127.0.0.1:5001/api/campaigns', json=new_camp)
        print("POST /api/campaigns ->", r.status_code, r.text)
