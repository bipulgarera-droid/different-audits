import requests

audit_id = "6e05ec0b-38e7-45d8-9000-9b574178626d"
res = requests.post(f"http://127.0.0.1:5001/api/audits/{audit_id}/generate-slides")

print("STATUS CODE:", res.status_code)
print("RESPONSE:", res.text)
