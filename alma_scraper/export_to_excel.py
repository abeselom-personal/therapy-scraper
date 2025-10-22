import os
from datetime import datetime

import pandas as pd
from pymongo import MongoClient


def get_mongo_client():
    """Create MongoDB client from environment variables."""
    mongo_host = os.getenv("MONGO_HOST", "mongodb")
    mongo_port = int(os.getenv("MONGO_PORT", "27017"))
    mongo_db = os.getenv("MONGO_DB", "alma_scraper_final")
    mongo_user = os.getenv("MONGO_USER", "scraper")
    mongo_password = os.getenv("MONGO_PASSWORD", "scraper")

    conn = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"
    return MongoClient(conn)


def export_alma_to_excel():
    """Export Alma therapist data to Excel."""
    client = get_mongo_client()
    db = client[os.getenv("MONGO_DB", "alma_scraper_final")]

    therapists = list(db.therapists.find())
    if not therapists:
        print("No therapists found in database")
        return

    # Create DataFrame
    df = pd.DataFrame(therapists)

    # Remove MongoDB _id field
    if "_id" in df.columns:
        df = df.drop("_id", axis=1)

    # Ensure exports directory exists
    os.makedirs("exports", exist_ok=True)

    # Export to Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"exports/alma_export_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    print(f"Excel exported: {filename} | {len(df)} records")

    client.close()


if __name__ == "__main__":
    export_alma_to_excel()
