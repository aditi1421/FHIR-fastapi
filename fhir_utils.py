import httpx
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
FHIR_base_url = os.getenv("FHIR_base_url")

# Fetch patient data with pagination
async def fetch_patients(limit=500000):
    patients = []
    url = f"{FHIR_base_url}/Patient?_count=50"
    total_fetched = 0

    async with httpx.AsyncClient() as client:
        while url and total_fetched < limit:
            response = await client.get(url)
            response.raise_for_status()
            bundle = response.json()

            entries = bundle.get("entry", [])
            patients.extend(entries)
            total_fetched += len(entries)

            # Pagination: check if there's a 'next' page
            next_link = next(
                (link["url"] for link in bundle.get("link", []) if link["relation"] == "next"),
                None
            )
            url = next_link

    return {"entry": patients[:limit]}

# Extract patient data into a DataFrame
def extract_patient_data(fhir_data):
    entries = fhir_data.get("entry", [])
    rows = []

    for entry in entries:
        resource = entry.get("resource", {})
        patient_id = resource.get("id", "Unknown")
        name_info = resource.get("name", [{}])[0]

        given = " ".join(name_info.get("given", [])) if "given" in name_info else ""
        family = name_info.get("family", "")
        full_name = f"{given} {family}".strip() or "Unknown"

        gender = resource.get("gender", "Unknown")
        birth_date = resource.get("birthDate", "Unknown")

        rows.append({
            "ID": patient_id,
            "Name": full_name,
            "Gender": gender,
            "Birth Date": birth_date
        })

    df = pd.DataFrame(rows)

    #Don't remove based on ID — instead drop based on (Name, Gender, Birth Date)
    df = df[df["Name"].str.strip().str.lower() != "unknown"]
    df = df[~df["Name"].str.lower().str.contains("test|demo|sample")]
    df = df.drop_duplicates(subset=["Name", "Gender", "Birth Date"])

    return df