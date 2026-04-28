import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import base64
import os

# =========================
# ENV VARIABLES (GitHub Secrets)
# =========================
RC_CLIENT_ID = os.environ['RC_CLIENT_ID']
RC_CLIENT_SECRET = os.environ['RC_CLIENT_SECRET']
RC_JWT = os.environ['RC_JWT']

FILE_PATH = "ringcentral_call_logs.csv"

# =========================
# 1. AUTH
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
data_token = res.json()

if "access_token" not in data_token:
    raise Exception(f"Auth failed: {data_token}")

access_token = data_token["access_token"]

print("✅ Auth success")

# =========================
# 2. FETCH CALL LOGS
# =========================
date_from = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

api_url = "https://platform.ringcentral.com/restapi/v1.0/account/~/call-log"

headers = {
    "Authorization": f"Bearer {access_token}"
}

params = {
    "dateFrom": date_from
}

response = requests.get(api_url, headers=headers, params=params)
data = response.json()
records = data.get("records", [])

df = pd.json_normalize(records)

# =========================
# 3. SDR LOGIC
# =========================
if not df.empty:
    df["SDR_Name"] = np.where(
        df["direction"] == "Outbound",
        df["from.name"],
        df["to.name"]
    )
else:
    print("⚠️ No new records")

# =========================
# 4. INCREMENTAL LOGIC
# =========================
if os.path.exists(FILE_PATH):
    old_df = pd.read_csv(FILE_PATH)
    final_df = pd.concat([old_df, df])

    if "id" in final_df.columns:
        final_df = final_df.drop_duplicates(subset=["id"])
else:
    final_df = df

# =========================
# 5. SAVE
# =========================
final_df.to_csv(FILE_PATH, index=False)

print("✅ CSV Updated")
