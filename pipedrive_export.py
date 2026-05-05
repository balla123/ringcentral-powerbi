import requests
import pandas as pd
import os

# =========================
# CONFIG
# =========================
API_TOKEN = "f80fda77952cf41dd693705f93251178c70d7681"

LEADS_URL = "https://api.pipedrive.com/v1/leads"
PERSON_URL = "https://api.pipedrive.com/v1/persons"
ORG_URL = "https://api.pipedrive.com/v1/organizations"
USERS_URL = "https://api.pipedrive.com/v1/users"
ACTIVITY_URL = "https://api.pipedrive.com/v1/activities"
FIELDS_URL = f"https://api.pipedrive.com/v1/personFields?api_token={API_TOKEN}"

CALL_LABEL_KEY = "6fb63814f3bd7ff09a6ad92d3e4abe3d4955ad07"


# =========================
# PAGINATION
# =========================
def fetch_all(url):
    all_data = []
    start = 0
    limit = 100

    while True:
        params = {
            "api_token": API_TOKEN,
            "start": start,
            "limit": limit
        }

        res = requests.get(url, params=params).json()
        data = res.get("data", [])

        if not data:
            break

        all_data.extend(data)

        pagination = res.get("additional_data", {}).get("pagination", {})
        if not pagination.get("more_items_in_collection"):
            break

        start += limit

    return all_data


# =========================
# HELPERS
# =========================
def extract_name(field):
    if isinstance(field, dict):
        return field.get("name", "")
    return ""


def extract_id(field):
    if isinstance(field, dict):
        return field.get("value") or field.get("id")
    return field


# =========================
# DROPDOWN DECODER
# =========================
def get_option_map(field_key):
    fields = requests.get(FIELDS_URL).json().get("data", [])
    for f in fields:
        if f["key"] == field_key:
            return {
                str(opt["id"]): opt["label"]
                for opt in f.get("options", [])
            }
    return {}


def decode_value(raw, option_map):
    if isinstance(raw, list):
        return ", ".join([option_map.get(str(v), str(v)) for v in raw])
    return option_map.get(str(raw), str(raw))


# =========================
# MAPPINGS
# =========================
def get_user_map():
    users = fetch_all(USERS_URL)
    return {u["id"]: u["name"] for u in users}


def get_person_map():
    persons = fetch_all(PERSON_URL)
    return {p["id"]: p["name"] for p in persons}


def get_org_map():
    orgs = fetch_all(ORG_URL)
    return {o["id"]: o["name"] for o in orgs}


# =========================
# CONTACTS (FIXED)
# =========================
def fetch_contacts():
    persons = fetch_all(PERSON_URL)
    option_map = get_option_map(CALL_LABEL_KEY)

    rows = []
    for p in persons:
        email = p.get("email", [])
        phone = p.get("phone", [])

        raw_call_label = p.get(CALL_LABEL_KEY, "")
        call_label = decode_value(raw_call_label, option_map)

        rows.append({
            "Person Name": p.get("name", ""),
            "Email": email[0]["value"] if email else "",
            "Phone": phone[0]["value"] if phone else "",
            "Tag": str(p.get("a73ad09d182b53e7aae4d2cc45213a206fdf05ba", "")),
            "Call Label": call_label,
            "Owner": extract_name(p.get("owner_id")),
            "Organization": extract_name(p.get("org_id"))
        })

    df = pd.DataFrame(rows).fillna("").astype(str)
    return df[df["Owner"].str.lower() == "christine maitland"]


# =========================
# ACTIVITIES
# =========================
def fetch_activities():
    activities = fetch_all(ACTIVITY_URL)

    rows = []
    for a in activities:
        rows.append({
            "Activity ID": a.get("id"),
            "Subject": a.get("subject", ""),
            "Type": a.get("type", ""),
            "Status": "Done" if a.get("done") == 1 else "Pending",
            "Add Time": a.get("add_time", ""),
            "Person Name": extract_name(a.get("person_id")),
            "Owner": extract_name(a.get("owner_id"))
        })

    return pd.DataFrame(rows).fillna("").astype(str)


# =========================
# LEADS (HYBRID FIX)
# =========================
def fetch_leads():
    leads = fetch_all(LEADS_URL)

    # mappings
    person_map = get_person_map()
    org_map = get_org_map()
    user_map = get_user_map()
    label_map = get_lead_label_map()   # 🔥 NEW

    all_rows = []

    for lead in leads:
        person_id = extract_id(lead.get("person_id"))
        org_id = extract_id(lead.get("organization_id"))
        owner_id = extract_id(lead.get("owner_id"))

        raw_labels = lead.get("labels", [])

        labels = decode_labels(raw_labels, label_map)   # 🔥 FIX

        all_rows.append({
            "Lead ID": lead.get("id"),
            "Title": lead.get("title", ""),
            "Status": lead.get("status", ""),
            "Source": lead.get("source_name", ""),
            "Add Time": lead.get("add_time", ""),

            # ✅ names
            "Person Name": lead.get("person_name") or person_map.get(person_id, ""),
            "Organization": lead.get("org_name") or org_map.get(org_id, ""),
            "Owner": user_map.get(owner_id, ""),

            # ✅ NEW
            "Labels": labels,
            "Deal Value": lead.get("deal_value", "")
        })

    df = pd.DataFrame(all_rows).fillna("").astype(str)

    return df
    # =========================
# MAIN
# =========================
def main():
    df_contacts = fetch_contacts()
    df_activities = fetch_activities()
    df_leads = fetch_leads()

    with pd.ExcelWriter("pipedrive_data.xlsx", engine="openpyxl") as writer:
        df_contacts.to_excel(writer, sheet_name="Contacts", index=False)
        df_activities.to_excel(writer, sheet_name="Activities", index=False)
        df_leads.to_excel(writer, sheet_name="Leads", index=False)

    print("✅ Excel created successfully")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    main()
