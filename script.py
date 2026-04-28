import requests
import pandas as pd
from datetime import datetime, timedelta
import base64
import os

# =========================
# ENV VARIABLES (from GitHub Secrets)
# =========================
RC_CLIENT_ID = os.environ['RC_CLIENT_ID']
RC_CLIENT_SECRET = os.environ['RC_CLIENT_SECRET']
RC_JWT = os.environ['RC_JWT']

TENANT_ID = os.environ['TENANT_ID']
MS_CLIENT_ID = os.environ['MS_CLIENT_ID']
MS_CLIENT_SECRET = os.environ['MS_CLIENT_SECRET']
USER_EMAIL = os.environ['USER_EMAIL']

FILE_PATH = "ringcentral_call_logs.csv"

# =========================
# 1. RINGCENTRAL AUTH
# =========================
token_url = "https://platform.ringcentral.com/restapi/oauth/token"

auth = base64.b64encode(f"{RC_CLIENT_ID}:{RC_CLIENT_SECRET}".encode()).decode()

headers = {
    "Authorization": f"Basic {auth}",
    "Content-Type": "application/x-www-form-urlencoded"
}

payload = {
    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
    "assertion": RC_JWT
}

res = requests.post(token_url, headers=headers, data=payload)
access_token = res.json()["access_token"]

# =========================
# 2. FETCH CALL LOGS (LAST 1 HOUR)
# =========================
date_from = (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

api_url = "https://platform.ringcentral.com/restapi/v1.0/account/~/call-log"

headers = {
    "Authorization": f"Bearer {access_token}"
}

params = {"dateFrom": date_from}

response = requests.get(api_url, headers=headers, params=params)
data = response.json()
records = data.get("records", [])

df = pd.json_normalize(records)

# =========================
# 3. SAVE CSV
# =========================
df.to_csv(FILE_PATH, index=False)

# =========================
# 4. MICROSOFT AUTH
# =========================
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

token_data = {
    "client_id": MS_CLIENT_ID,
    "client_secret": MS_CLIENT_SECRET,
    "scope": "https://graph.microsoft.com/.default",
    "grant_type": "client_credentials"
}

token_res = requests.post(token_url, data=token_data)
ms_token = token_res.json()["access_token"]

# =========================
# 5. UPLOAD TO ONEDRIVE
# =========================
upload_url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/ringcentral_call_logs.csv:/content"

headers = {
    "Authorization": f"Bearer {ms_token}"
}

with open(FILE_PATH, "rb") as f:
    requests.put(upload_url, headers=headers, data=f)

print("Upload successful!")
