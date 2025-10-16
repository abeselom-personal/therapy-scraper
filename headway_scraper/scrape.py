import asyncio
import json
import os
import random
from datetime import datetime, timedelta

from playwright.async_api import async_playwright
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "headway_scraper_final")
MONGO_USER = os.getenv("MONGO_USER", "scraper")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "scraper")
# MongoDB
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
print(MONGO_URI)
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("[INFO] MongoDB connection successful")
except ConnectionFailure as e:
    print(f"[ERROR] MongoDB connection failed: {e}")
    exit(1)

db = client[MONGO_DB]

STATES = [
    "alaska",
    "montana",
    "arkansas",
    "minnesota",
    "nebraska",
    "delaware",
    "kansas",
    "new-hampshire",
    "california",
    "mississippi",
    "illinois",
    "indiana",
    "missouri",
    "kentucky",
    "north-carolina",
    "new-jersey",
    "colorado",
    "maryland",
    "michigan",
    "connecticut",
    "dc",
    "arizona",
    "louisiana",
    "massachusetts",
    "ohio",
    "alabama",
    "iowa",
    "south-dakota",
    "hawaii",
    "georgia",
    "florida",
    "texas",
    "virginia",
    "pennsylvania",
    "washington",
    "west-virginia",
    "tennessee",
    "idaho",
    "oregon",
    "new-york",
    "maine",
    "wyoming",
    "nevada",
    "wisconsin",
    "north-dakota",
    "vermont",
    "utah",
    "new-mexico",
    "rhode-island",
    "south-carolina",
    "oklahoma",
]


async def fetch_availability_with_playwright(page, clinician_id):
    start = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    end = (datetime.utcnow() + timedelta(days=14)).replace(
        hour=23, minute=59, second=59, microsecond=999000
    ).isoformat() + "Z"

    url = (
        f"https://care.headway.co/api-proxy/provider/{clinician_id}/availability"
        f"?date_range_start={start}&date_range_end={end}"
        f"&is_followup_appointment=false&has_completed_intake_session=false"
    )
    print(f"[DEBUG] Fetching availability for clinician_id={clinician_id}")
    print(f"[DEBUG] Request URL: {url}")

    try:
        data = await page.evaluate(
            """async (url) => {
                try {
                    const resp = await fetch(url, { credentials: "include" });
                    const contentType = resp.headers.get("content-type") || "";
                    if (!contentType.includes("application/json")) {
                        const text = await resp.text();
                        return { error: "Unexpected content-type", content: text.substring(0, 500) };
                    }
                    return await resp.json();
                } catch (err) {
                    return { error: err.toString() };
                }
            }""",
            url,
        )

        if isinstance(data, list):
            availability_count = len(data)
        elif isinstance(data, dict):
            availability_count = len(data.get("availability", []))
        else:
            availability_count = 0

        if "error" in data:
            print(f"[WARN] Failed to fetch availability: {data['error']}")
            if "content" in data:
                print(f"[DEBUG] Response preview: {data['content']}")
        else:
            print(
                f"[DEBUG] Successfully fetched availability, entries: {availability_count}"
            )

        return data

    except Exception as e:
        print(f"[ERROR] Exception in fetch_availability_with_playwright: {e}")
        return {"error": str(e)}


def process_clinician(c, state, all_specialties):
    # Create mappings from the allSpecialties data
    specialty_mapping = {
        spec["id"]: spec["patientDisplayName"] for spec in all_specialties
    }

    # Map focusAreas IDs to names
    focus_areas_names = []
    for area_id in c.get("focusAreas", []):
        focus_areas_names.append(
            specialty_mapping.get(area_id, f"Unknown Area {area_id}")
        )

    # Map otherSpecialties IDs to names
    other_specialties_names = []
    for spec_id in c.get("otherSpecialties", []):
        other_specialties_names.append(
            specialty_mapping.get(spec_id, f"Unknown Specialty {spec_id}")
        )

    # Combine focus areas and other specialties for "Additional Focus Areas"
    all_focus_areas = focus_areas_names + other_specialties_names

    # Map modalities (treatment approaches) - these would need their own mapping
    # Since we don't have the modalities mapping in the data, I'll leave them as IDs for now
    # You would need to create a similar mapping for modalities
    treatment_approaches = c.get("modalities", [])

    # Map age groups
    age_group_mapping = {
        1: "Children (0-5)",
        2: "Children (6-12)",
        3: "Teens (13-18)",
        4: "Adults (19-64)",
        5: "Seniors (65+)",
    }
    age_groups_names = []
    for age_id in c.get("treatableAgeGroups", []):
        age_groups_names.append(
            age_group_mapping.get(age_id, f"Unknown Age Group {age_id}")
        )

    # Extract insurance information
    insurance_ids = c.get("searchProviderLicenseState", {}).get(
        "frontEndCarrierIds", []
    )
    insurance_mapping = {
        1: "Aetna",
        3: "Cigna",
        276: "United Healthcare",
        282: "Blue Cross Blue Shield",
        # Add more mappings as needed based on your data
    }
    accepted_insurances = []
    for ins_id in insurance_ids:
        accepted_insurances.append(
            insurance_mapping.get(ins_id, f"Insurance {ins_id}")
        )

    # Extract location information
    locations = []
    if c.get("searchProviderLicenseState", {}).get("locations"):
        for loc in c["searchProviderLicenseState"]["locations"]:
            locations.append(
                f"{loc.get('streetAddress', '')}, {loc.get('state', '')}"
            )

    return {
        "clinician_id": c.get("providerId"),
        "Url": f"https://care.headway.co/providers/{c.get('slug', '')}",
        "Name": c.get("displayName"),
        "NPI": c.get("npiNumber"),  # Note: This field might not be in your data
        "Profession": c.get("patientViewableProviderType"),
        "Clinic Name": c.get(
            "clinicName"
        ),  # Note: This field might not be in your data
        "Bio": clean_html(
            c.get("bioAboutYou", "")
        ),  # Added HTML cleaning function
        "Additional Focus Areas": ", ".join(all_focus_areas),
        "Treatment Approaches": ", ".join(
            map(str, treatment_approaches)
        ),  # Would need modalities mapping
        "Appointment Types": "Telehealth",  # Based on telehealthAvailabilityCount > 0
        "Communities": ", ".join(c.get("ethnicity", [])),
        "Age Groups": ", ".join(age_groups_names),
        "Languages": ", ".join(c.get("languages", [])),
        "Highlights": ", ".join(c.get("styleTags", [])),
        "Gender": c.get("gender"),
        "Pronouns": c.get("pronouns"),
        "Race Ethnicity": ", ".join(c.get("ethnicity", [])),
        "Licenses": c.get("licenseType", ""),
        "Locations": (
            "; ".join(locations) if locations else c.get("location", "")
        ),
        "Education": f"{c.get('degreeType', '')} - {c.get('school', '')}".strip(
            " - "
        ),
        "Faiths": "",  # Not in your data
        "Min Session Price": "",  # Not in your data
        "Max Session Price": "",  # Not in your data
        "Pay Out Of Pocket Status": "",  # Not in your data
        "Individual Service Rates": "",  # Not in your data
        "General Payment Options": "Insurance",  # Based on frontEndCarrierIds
        "Booking Summary": f"Next availability: {c.get('nextAvailabilityDateWithinTwoWeeks', '')}",
        "Booking Url": f"https://care.headway.co/providers/{c.get('slug', '')}",
        "Listed In States": ", ".join(
            [
                state.get("state", "")
                for state in c.get("activeProviderStates", [])
            ]
        ),
        "States": state,
        "Listed In Websites": "Headway",
        "Urls": c.get("photoUrl"),
        "Connect Link - Facebook": "",  # Not in your data
        "Connect Link - Instagram": "",  # Not in your data
        "Connect Link - LinkedIn": "",  # Not in your data
        "Connect Link - Twitter": "",  # Not in your data
        "Connect Link - Website": "",  # Not in your data
        "Main Specialties": ", ".join(focus_areas_names),
        "Accepted IPs": ", ".join(accepted_insurances),
        "Sr. NO": c.get("objectID"),  # Using objectID as sequence number
        "scraped_at": datetime.utcnow(),
    }


def clean_html(text):
    """Remove HTML tags from text"""
    if not text:
        return ""
    import re

    clean = re.compile("<.*?>")
    return re.sub(clean, "", text)


async def scrape_state(state, currentPage=1):
    print(f"--- Starting scrape for {state} ---")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        url = f"https://care.headway.co/therapists/{state}?page={currentPage}&_data=routes%2FseoDirectory%2Flocations"
        if currentPage == 1:
            url = f"https://care.headway.co/therapists/{state}?_data=routes%2FseoDirectory%2Flocations"

        print(f"[INFO] Navigating to URL: {url}")
        try:
            response = await page.goto(url, wait_until="domcontentloaded")
            print(
                f"[INFO] HTTP response status: {response.status if response else 'No response'}"
            )
        except Exception as e:
            print(f"[ERROR] Failed to load page for {state}: {e}")
            await browser.close()
            return 0

        try:
            pre = await page.query_selector("pre")
            if not pre:
                print(f"[WARN] No JSON <pre> tag found for {state}")
                await browser.close()
                return 0

            content = await pre.text_content()
            print(
                f"[INFO] Extracted JSON content, length: {len(content)} characters"
            )
            data = json.loads(content)
        except Exception as e:
            print(f"[ERROR] Failed to extract/parse JSON for {state}: {e}")
            await browser.close()
            return 0

        clinicians = data.get("topProviders", [])
        all_specialties = data.get("allSpecialties", [])
        print(f"[INFO] Found {len(clinicians)} clinician records for {state}")
        totalPages = data.get("totalPages", 1)
        print(f"[INFO] Found {totalPages} Total Pages for {state}")

        batch = [
            process_clinician(c, state, all_specialties) for c in clinicians
        ]

        # Fetch availability for each clinician
        for c in batch:
            c["availability"] = await fetch_availability_with_playwright(
                page, c["clinician_id"]
            )

        if batch:
            try:
                ops = [
                    UpdateOne(
                        {"clinician_id": c["clinician_id"]},
                        {"$set": c},
                        upsert=True,
                    )
                    for c in batch
                ]
                result = db.clinicians.bulk_write(ops, ordered=False)
                print(
                    f"[INFO] Upserted {result.upserted_count} new records, modified {result.modified_count} records"
                )
                raw_doc = {
                    "state": state,
                    "page": currentPage,
                    "scraped_at": datetime.utcnow(),
                    "raw_data": data,
                }
                db.raw_pages.update_one(
                    {"state": state, "page": currentPage},
                    {"$set": raw_doc},
                    upsert=True,
                )
            except Exception as e:
                print(f"[ERROR] MongoDB bulk_write failed for {state}: {e}")

        await browser.close()
        print(
            f"--- Finished scrape for {state}, total saved: {len(batch)} ---\n"
        )
        return totalPages - currentPage


async def main():
    for state in STATES:
        page = 1
        pagesLeft = 1
        while pagesLeft > 0:
            pagesLeft = await scrape_state(state, page)
            page += 1
        await asyncio.sleep(random.uniform(2, 5))
    print("All done!")


if __name__ == "__main__":
    asyncio.run(main())
