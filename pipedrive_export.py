import requests
import pandas as pd
import os

API_TOKEN ="f80fda77952cf41dd693705f93251178c70d7681"


# =========================
# COMMON PAGINATION
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
# SAFE HELPERS
# =========================
def extract_name(field):
    """Handles dict / None safely"""
    if isinstance(field, dict):
        return field.get("name", "")
    return ""


def extract_id(field):
    """Handles dict / int safely"""
    if isinstance(field, dict):
        return field.get("value") or field.get("id")
    return field


# =========================
# CONTACTS
# =========================
def fetch_contacts():
    persons = fetch_all("https://api.pipedrive.com/v1/persons")

    rows = []
    for p in persons:
        email = p.get("email", [])
        phone = p.get("phone", [])

        rows.append({
            "Person Name": p.get("name", ""),
            "Email": email[0]["value"] if email else "",
            "Phone": phone[0]["value"] if phone else "",
            "Tag": str(p.get("a73ad09d182b53e7aae4d2cc45213a206fdf05ba", "")),
            "Call Label": str(p.get("6fb63814f3bd7ff09a6ad92d3e4abe3d4955ad07", "")),

            # ✅ SAFE EXTRACTION
            "Owner": extract_name(p.get("owner_id")),
            "Organization": extract_name(p.get("org_id"))
        })

    df = pd.DataFrame(rows).fillna("").astype(str)

    return df[df["Owner"].str.lower() == "christine maitland"]


# =========================
# ACTIVITIES
# =========================
def fetch_activities():
    activities = fetch_all("https://api.pipedrive.com/v1/activities")

    rows = []
    for a in activities:
        rows.append({
            "Activity ID": a.get("id"),
            "Subject": a.get("subject", ""),
            "Type": a.get("type", ""),
            "Status": "Done" if a.get("done") == 1 else "Pending",
            "Add Time": a.get("add_time", ""),

            # ✅ SAFE
            "Person Name": extract_name(a.get("person_id")),
            "Owner": extract_name(a.get("owner_id"))
        })

    return pd.DataFrame(rows).fillna("").astype(str)


# =========================
# LEADS
# =========================
def fetch_leads():
    leads = fetch_all("https://api.pipedrive.com/v1/leads")

    # mappings (only used as fallback)
    user_map = get_user_map()
    person_map = get_person_map()
    org_map = get_org_map()

    def extract_id(field):
        if isinstance(field, dict):
            return field.get("value") or field.get("id")
        return field

    rows = []

    for lead in leads:
        owner_id = extract_id(lead.get("owner_id"))
        creator_id = extract_id(lead.get("creator_user_id"))

        person_id = extract_id(lead.get("person_id"))
        org_id = extract_id(lead.get("organization_id"))

        # 🔥 HYBRID FIX
        person_name = lead.get("person_name") or person_map.get(person_id, "")
        org_name = lead.get("org_name") or org_map.get(org_id, "")

        rows.append({
            "Lead ID": lead.get("id"),
            "Title": lead.get("title", ""),
            "Status": lead.get("status", ""),
            "Source": lead.get("source_name", ""),
            "Add Time": lead.get("add_time", ""),

            "Person Name": person_name,
            "Organization": org_name,

            "Owner": user_map.get(owner_id, ""),
            "Creator": user_map.get(creator_id, "")
        })

    df = pd.DataFrame(rows).fillna("").astype(str)

    return df

# =========================
# MAIN EXPORT
# =========================
def main():
    df_contacts = fetch_contacts()
    df_activities = fetch_activities()
    df_leads = fetch_leads()

    with pd.ExcelWriter("pipedrive_data.xlsx", engine="openpyxl") as writer:
        df_contacts.to_excel(writer, sheet_name="Contacts", index=False)
        df_activities.to_excel(writer, sheet_name="Activities", index=False)
        df_leads.to_excel(writer, sheet_name="Leads", index=False)

    print("✅ Excel file created successfully")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    main()
