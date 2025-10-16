import os
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "heloalma_scraper_final")
MONGO_USER = os.getenv("MONGO_USER", "scraper")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "scraper")
OUTPUT_DIR = "./exports/helloalma"


def get_mongo_client():
    conn = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
    return MongoClient(conn)


def flatten_clinician_data(c):
    return {
        "clinician_id": c.get("clinician_id", ""),
        "Url": "",
        "Name": f"{c.get('first_name', '')} {c.get('last_name', '')}".strip(),
        "Profession": ", ".join(c.get("title", [])),
        "Clinic Name": "",
        "Bio": (c.get("bio_about_you", "") or "")
        + " "
        + (c.get("bio_therapy_approach", "") or ""),
        "Additional Focus Areas": ", ".join(map(str, c.get("focus_areas", []))),
        "Treatment Approaches": "humanistic, cognitive-behavioral, developmental, solution-focused, holistic, trauma-informed, strengths-based",
        "Appointment Types": "telehealth" if c.get("telehealth") else "",
        "Communities": "",
        "Age Groups": "",
        "Languages": ", ".join(c.get("languages", [])),
        "Highlights": ", ".join(c.get("style_tags", [])),
        "Gender": "",
        "Pronouns": "",
        "Race Ethnicity": ", ".join(c.get("ethnicity", [])),
        "Licenses": ", ".join(c.get("title", [])),
        "Locations": c.get("location", ""),
        "Education": "",
        "Faiths": "",
        "Min Session Price": "",
        "Max Session Price": "",
        "Pay Out Of Pocket Status": "",
        "Individual Service Rates": "",
        "General Payment Options": "",
        "Booking Summary": "",
        "Booking Url": "",
        "Listed In States": ", ".join(
            [s.get("state", "") for s in c.get("active_states", [])]
        ),
        "States": c.get("location_state", ""),
        "Listed In Websites": "headway.co",
        "Urls": c.get("profile_img", ""),
        "Connect Link - Facebook": "",
        "Connect Link - Instagram": "",
        "Connect Link - LinkedIn": "",
        "Connect Link - Twitter": "",
        "Connect Link - Website": "",
        "Main Specialties": ", ".join(map(str, c.get("focus_areas", []))),
        "Accepted IPs": "",
        "Sr. NO": "",
    }


def export_clinicians_to_excel():
    client = get_mongo_client()
    db = client[MONGO_DB]
    clinicians = list(db.clinicians.find())
    if not clinicians:
        print("No clinicians found")
        return

    flattened = [flatten_clinician_data(c) for c in clinicians]
    for i, f in enumerate(flattened, start=1):
        f["Sr. NO"] = i

    df = pd.DataFrame(flattened)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"headway_{timestamp}.xlsx")
    df.to_excel(filename, index=False)
    print(f"Excel exported: {filename} | {len(df)} records")
    client.close()


if __name__ == "__main__":
    export_clinicians_to_excel()
