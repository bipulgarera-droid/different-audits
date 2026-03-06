import sys
import os
from dotenv import load_dotenv

load_dotenv()

# We can bypass imports and directly call the python method 
sys.path.append(os.getcwd())
from api.dataforseo_client import start_onpage_audit

res = start_onpage_audit("rank-jacker.com", max_pages=10)
print("DataForSEO Response:", res)
