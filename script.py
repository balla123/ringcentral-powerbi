import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import base64
import os

# =========================
# 🔑 YOUR CREDENTIALS
# =========================
RC_CLIENT_ID = "Woq03G5Xe3AcnTmPOfY8Zx"
RC_CLIENT_SECRET = "cfUpIy6TDMDf0rKn6d4efrfCdta60HXjcbfwdTxbVWFV"
RC_JWT = "eyJraWQiOiI4NzYyZjU5OGQwNTk0NGRiODZiZjVjYTk3ODA0NzYwOCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJhdWQiOiJodHRwczovL3BsYXRmb3JtLnJpbmdjZW50cmFsLmNvbS9yZXN0YXBpL29hdXRoL3Rva2VuIiwic3ViIjoiNzkyNzcyMDM1IiwiaXNzIjoiaHR0cHM6Ly9wbGF0Zm9ybS5yaW5nY2VudHJhbC5jb20iLCJleHAiOjM5MjQ4NDE3OTcsImlhdCI6MTc3NzM1ODE1MCwianRpIjoiV0pfS2ZMNWlSNGlrbWZ5ZWhHWFZEUSJ9.DVxxxGNC8FInB_2ls3TxuJU9wOk8j_BXDq7n59Y5naVKV4qR-CyrlY98uU0x1jaFLSL87jYg00ir1G6AMvvC9tn8qwfvVS9FdUQSDwtJ4BBjtjWw3UbiwmjC07dRfZ93wvO40PvvjuuJxbkb6OZSE4ewHU6g0TxpCXJvzcf-ajmqnfyG8JS2wiCciLYMMHmYdtcj_qRPDlgnnIk7C6fiDK3Z9Cwptu56RhFUBoWh8TC0ROCTBhMXwd-f3PF70pDlbBOhgDKj2XjOZzVJF1c1gYOfCMlj8GUaGT_phaZVwYu6ryGzEhcwLtt_l62nzKoBaQJlDKmmfyknJbduHe8XYg"

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
# 2. GET LAST TIMESTAMP
# =========================
if os.path.exists(FILE_PATH):
    old_df = pd.read_csv(FILE_PATH)

    if not old_df.empty and "startTime" in old_df.columns:
        last_time = old_df["startTime"].max()
    else:
        last_time = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
else:
    old_df = pd.DataFrame()
    last_time = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

print("Fetching data after:", last_time)

# =========================
# 3. FETCH NEW DATA
# =========================
api_url = "https://platform.ringcentral.com/restapi/v1.0/account/~/call-log"

headers = {
    "Authorization": f"Bearer {access_token}"
}

params = {
    "dateFrom": last_time
}

response = requests.get(api_url, headers=headers, params=params)
data = response.json()
records = data.get("records", [])

new_df = pd.json_normalize(records)

# =========================
# 4. SDR LOGIC
# =========================
if not new_df.empty:
    new_df["SDR_Name"] = np.where(
        new_df["direction"] == "Outbound",
        new_df["from.name"],
        new_df["to.name"]
    )

# =========================
# 5. ADD INGEST TIMESTAMP (your idea 🔥)
# =========================
new_df["ingested_at"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

# =========================
# 6. APPEND + DEDUP
# =========================
final_df = pd.concat([old_df, new_df])

if "id" in final_df.columns:
    final_df = final_df.drop_duplicates(subset=["id"])

# =========================
# 7. SAVE
# =========================
final_df.to_csv(FILE_PATH, index=False)

print(f"✅ Added {len(new_df)} new records")
print(f"📊 Total records: {len(final_df)}")
