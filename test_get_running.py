import requests

# Test fetching the audit ID that is STILL running, which we discovered earlier
running_audit_id = "28b8836a-b8d6-4dba-85e6-e24bfb0a4ed4"
res = requests.get(f"http://127.0.0.1:5001/api/audits/{running_audit_id}")

print("STATUS CODE:", res.status_code)
data = res.json()
print("DATA:", list(data.keys()))

if "error" in data:
    print("ERROR MSG:", data["error"])
else:
    print("SUCCESS: Returned Audit", data["audit"]["status"])
