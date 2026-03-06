import requests

audit_id = "6e05ec0b-38e7-45d8-9000-9b574178626d"
res = requests.get(f"http://127.0.0.1:5001/api/audits/{audit_id}")

print("STATUS CODE:", res.status_code)
data = res.json()
print("KEYS:", data.keys())

if "error" in data:
    print("ERROR:", data["error"])
else:
    audit = data.get("audit", {})
    print("AUDIT STATUS:", audit.get("status"))
