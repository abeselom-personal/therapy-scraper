import gc
import os
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "headway_speed_test")
MONGO_USER = os.getenv("MONGO_USER", "scraper")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "scraper")
OUTPUT_DIR = "/app/exports/headway/"


def get_mongo_client():
    conn = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
    return MongoClient(conn)


def parse_datetime(dt_string):
    """Safely parse datetime string with timezone handling"""
    if not dt_string:
        return None

    try:
        # Handle ISO format with timezone
        if dt_string.endswith("Z"):
            dt_string = dt_string[:-1] + "+00:00"

        # Parse as timezone-aware datetime
        return datetime.fromisoformat(dt_string)
    except (ValueError, AttributeError):
        return None


def format_availability_summary(availability):
    """Format availability in the format: 'wed 12:20, wed 11:20'"""
    if not availability:
        return ""

    # Get current time as timezone-aware datetime
    now_aware = datetime.now(timezone.utc)

    # Collect future time slots
    time_slots = []
    for slot in availability:
        start_date_str = slot.get("startDate")
        if start_date_str:
            start_dt = parse_datetime(start_date_str)
            if start_dt and start_dt > now_aware:
                # Format: day abbreviation (lowercase) + space + time in 12-hour format without leading zero
                day_abbr = start_dt.strftime("%a").lower()  # "wed"
                time_str = start_dt.strftime(
                    "%-I:%M"
                )  # "12:20" (remove leading zero)
                time_slots.append(f"{day_abbr} {time_str}")

    # Remove duplicates and limit to reasonable number for display
    unique_slots = list(
        dict.fromkeys(time_slots)
    )  # Preserve order while removing duplicates
    return ", ".join(unique_slots[:10])  # Show first 10 unique slots


def flatten_clinician_data(c, row_number):
    # Extract first name from full name
    full_name = c.get("Name", "")
    first_name = full_name.split()[0] if full_name else ""

    # Process availability to get formatted summary
    availability = c.get("availability", [])
    availability_summary = format_availability_summary(availability)

    # Also get next available date for Booking Summary
    next_available = ""
    if availability:
        now_aware = datetime.now(timezone.utc)
        future_slots = []
        for slot in availability:
            start_date_str = slot.get("startDate")
            if start_date_str:
                start_dt = parse_datetime(start_date_str)
                if start_dt and start_dt > now_aware:
                    future_slots.append(slot)

        if future_slots:
            earliest_slot = min(
                future_slots, key=lambda x: parse_datetime(x["startDate"])
            )
            next_available = earliest_slot["startDate"]

    # Process treatment approaches
    treatment_approaches = c.get("Treatment Approaches", "")

    # Handle Sr field which appears to be a nested object
    sr_no = ""
    sr_field = c.get("Sr", {})
    if isinstance(sr_field, dict):
        sr_no = sr_field.get(" NO", "")
    else:
        sr_no = str(sr_field)

    # Handle scraped_at field
    scraped_at = ""
    scraped_field = c.get("scraped_at", {})
    if isinstance(scraped_field, dict):
        scraped_at = scraped_field.get("$date", "")
    else:
        scraped_at = str(scraped_field)

    return {
        "clinician_id": c.get("clinician_id", ""),
        "Url": c.get("Url", ""),
        "Name": c.get("Name", ""),
        "NPI": c.get("NPI", ""),
        "Profession": c.get("Profession", ""),
        "Clinic Name": c.get("Clinic Name", ""),
        "Bio": c.get("Bio", ""),
        "Additional Focus Areas": c.get("Additional Focus Areas", ""),
        "Treatment Approaches": treatment_approaches,
        "Appointment Types": c.get("Appointment Types", ""),
        "Communities": c.get("Communities", ""),
        "Age Groups": c.get("Age Groups", ""),
        "Languages": c.get("Languages", ""),
        "Highlights": c.get("Highlights", ""),
        "Gender": c.get("Gender", ""),
        "Pronouns": c.get("Pronouns", ""),
        "Race Ethnicity": c.get("Race Ethnicity", ""),
        "Licenses": c.get("Licenses", ""),
        "Locations": c.get("Locations", ""),
        "Education": c.get("Education", ""),
        "Faiths": c.get("Faiths", ""),
        "Min Session Price": c.get("Min Session Price", ""),
        "Max Session Price": c.get("Max Session Price", ""),
        "Pay Out Of Pocket Status": c.get("Pay Out Of Pocket Status", ""),
        "Individual Service Rates": c.get("Individual Service Rates", ""),
        "General Payment Options": c.get("General Payment Options", ""),
        "Booking Summary": (
            f"Next availability: {next_available}"
            if next_available
            else c.get("Booking Summary", "")
        ),
        "Booking Url": c.get("Booking Url", ""),
        "Listed In States": c.get("Listed In States", ""),
        "States": c.get("States", ""),
        "Listed In Websites": c.get("Listed In Websites", ""),
        "Urls": c.get("Urls", ""),
        "Connect Link - Facebook": c.get("Connect Link - Facebook", ""),
        "Connect Link - Instagram": c.get("Connect Link - Instagram", ""),
        "Connect Link - LinkedIn": c.get("Connect Link - LinkedIn", ""),
        "Connect Link - Twitter": c.get("Connect Link - Twitter", ""),
        "Connect Link - Website": c.get("Connect Link - Website", ""),
        "Main Specialties": c.get("Main Specialties", ""),
        "Accepted IPs": c.get("Accepted IPs", ""),
        "Sr. NO": sr_no,
        "scraped_at": scraped_at,
        "Availability Summary": availability_summary,  # New field with formatted availability
    }


def export_clinicians_to_excel():
    client = get_mongo_client()
    db = client[MONGO_DB]

    # Get total count first
    total_clinicians = db.clinicians.count_documents({})
    print(f"Total clinicians in database: {total_clinicians}")

    # Process in batches to avoid memory issues
    batch_size = 2000
    all_flattened_data = []

    # Use cursor with batch_size for memory efficiency
    cursor = db.clinicians.find({}, batch_size=batch_size)

    try:
        for i, clinician in enumerate(cursor, 1):
            try:
                flattened = flatten_clinician_data(clinician, i)
                all_flattened_data.append(flattened)
            except Exception as e:
                print(f"Error processing clinician {i}: {e}")
                # Add basic data even if there's an error
                basic_data = {
                    "clinician_id": clinician.get("clinician_id", f"error_{i}"),
                    "Name": clinician.get("Name", ""),
                    "Sr. NO": i,
                    "Error": str(e),
                }
                all_flattened_data.append(basic_data)

            # Process in smaller batches and clear memory
            if i % batch_size == 0:
                print(f"Processed {i} clinicians...")
                gc.collect()

    finally:
        cursor.close()

    if not all_flattened_data:
        print("No clinicians found")
        client.close()
        return

    # Column order - updated to include Availability Summary
    column_order = [
        "clinician_id",
        "Url",
        "Name",
        "NPI",
        "Profession",
        "Clinic Name",
        "Bio",
        "Additional Focus Areas",
        "Treatment Approaches",
        "Appointment Types",
        "Communities",
        "Age Groups",
        "Languages",
        "Highlights",
        "Gender",
        "Pronouns",
        "Race Ethnicity",
        "Licenses",
        "Locations",
        "Education",
        "Faiths",
        "Min Session Price",
        "Max Session Price",
        "Pay Out Of Pocket Status",
        "Individual Service Rates",
        "General Payment Options",
        "Booking Summary",
        "Booking Url",
        "Listed In States",
        "States",
        "Listed In Websites",
        "Urls",
        "Connect Link - Facebook",
        "Connect Link - Instagram",
        "Connect Link - LinkedIn",
        "Connect Link - Twitter",
        "Connect Link - Website",
        "Main Specialties",
        "Accepted IPs",
        "Sr. NO",
        "scraped_at",
        "Availability Summary",  # New column
    ]

    # Create DataFrame and export
    df = pd.DataFrame(all_flattened_data)

    # Ensure all columns exist (in case of errors)
    for col in column_order:
        if col not in df.columns:
            df[col] = ""

    # Reorder columns
    df = df[column_order]

    # Export main Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"headway_{timestamp}.xlsx")
    df.to_excel(filename, index=False)
    print(f"Excel exported: {filename} | {len(df)} records")

    # Export to collection.xlsx with safe handling
    update_collection_file_safe(df, "headway", len(df))

    client.close()


def update_collection_file_safe(df, website_name, record_count):
    """Safely update collection.xlsx with proper error handling"""
    collection_file = "/app/exports/collection.xlsx"
    os.makedirs("/app/exports/", exist_ok=True)

    try:
        import shutil

        from openpyxl import load_workbook

        # If collection file doesn't exist, create it
        if not os.path.exists(collection_file):
            with pd.ExcelWriter(collection_file, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=website_name, index=False)
                analysis_df = pd.DataFrame(
                    {f"Total {website_name}": [record_count]}
                )
                analysis_df.to_excel(writer, sheet_name="analysis", index=False)
            print(
                f"Created new collection.xlsx with {record_count} {website_name} records"
            )
            return

        # If file exists, handle carefully
        try:
            # Try to read existing file structure
            existing_sheets = pd.ExcelFile(collection_file).sheet_names
            print(f"Existing sheets: {existing_sheets}")

            # Create new writer and copy all sheets except our target
            with pd.ExcelWriter(collection_file, engine="openpyxl") as writer:
                # Copy all existing sheets except our website sheet and analysis
                for sheet in existing_sheets:
                    if sheet != website_name and sheet != "analysis":
                        try:
                            sheet_df = pd.read_excel(
                                collection_file, sheet_name=sheet
                            )
                            sheet_df.to_excel(
                                writer, sheet_name=sheet, index=False
                            )
                        except Exception as e:
                            print(
                                f"Warning: Could not copy sheet '{sheet}': {e}"
                            )

                # Write our data
                df.to_excel(writer, sheet_name=website_name, index=False)

                # Update analysis sheet
                analysis_data = {}
                for sheet in existing_sheets:
                    if sheet != "analysis" and sheet != website_name:
                        try:
                            sheet_df = pd.read_excel(
                                collection_file, sheet_name=sheet
                            )
                            analysis_data[f"Total {sheet}"] = len(sheet_df)
                        except:
                            analysis_data[f"Total {sheet}"] = 0

                analysis_data[f"Total {website_name}"] = record_count
                analysis_df = pd.DataFrame([analysis_data])
                analysis_df.to_excel(writer, sheet_name="analysis", index=False)

            print(
                f"Updated collection.xlsx with {record_count} {website_name} records"
            )

        except Exception as e:
            print(
                f"Existing collection file appears corrupted, creating new one: {e}"
            )
            # Backup corrupted file
            backup_file = f"/app/exports/collection_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            try:
                shutil.copy2(collection_file, backup_file)
                print(f"Backed up corrupted file to: {backup_file}")
            except:
                pass

            # Create new file
            with pd.ExcelWriter(collection_file, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=website_name, index=False)
                analysis_df = pd.DataFrame(
                    {f"Total {website_name}": [record_count]}
                )
                analysis_df.to_excel(writer, sheet_name="analysis", index=False)
            print(
                f"Created new collection.xlsx with {record_count} {website_name} records"
            )

    except ImportError:
        print("openpyxl not available, skipping collection.xlsx update")
    except Exception as e:
        print(f"Error updating collection.xlsx: {e}")


if __name__ == "__main__":
    export_clinicians_to_excel()
