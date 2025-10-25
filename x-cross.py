import gc
import json
import os
from datetime import datetime

import pandas as pd
import requests


class NPIAnalysisProcessor:
    def __init__(self, input_file):
        self.input_file = input_file
        self.all_data = None
        self.npi_found_data = []
        self.npi_not_found_data = []
        self.analysis_data = []

    def load_data(self, limit=None):
        """Load data from the consolidated Excel file"""
        try:
            print("Loading consolidated data...")
            self.all_data = pd.read_excel(
                self.input_file, sheet_name="all_data"
            )

            if limit:
                self.all_data = self.all_data.head(limit)
                print(f"Limited to {limit} records for testing")

            print(f"✓ Loaded {len(self.all_data)} records")
            return True
        except Exception as e:
            print(f"Error loading data: {e}")
            return False

    def fetch_npi_data(self, npi_number):
        """Fetch NPI data from the CMS API"""
        try:
            if not npi_number or pd.isna(npi_number) or npi_number == "":
                return None

            # Convert to string and clean
            print(npi_number)
            npi_str = str(int(float(npi_number))).strip()
            if not npi_str.isdigit() or len(npi_str) != 10:
                return None

            url = "https://npiregistry.cms.hhs.gov/api/"
            params = {"version": "2.1", "number": npi_str}

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("result_count", 0) > 0:
                return data["results"][0]
            else:
                return None

        except Exception as e:
            print(f"Error fetching NPI {npi_number}: {e}")
            return None

    def extract_npi_details(self, npi_data):
        """Extract relevant details from NPI API response"""
        if not npi_data:
            return {}

        details = {
            "NPI_Number": npi_data.get("number", ""),
            "NPI_Type": npi_data.get("enumeration_type", ""),
            "NPI_Status": npi_data.get("basic", {}).get("status", ""),
            "First_Name": npi_data.get("basic", {}).get("first_name", ""),
            "Last_Name": npi_data.get("basic", {}).get("last_name", ""),
            "Middle_Name": npi_data.get("basic", {}).get("middle_name", ""),
            "Credential": npi_data.get("basic", {}).get("credential", ""),
            "Gender": npi_data.get("basic", {}).get("sex", ""),
            "Enumeration_Date": npi_data.get("basic", {}).get(
                "enumeration_date", ""
            ),
            "Last_Updated": npi_data.get("basic", {}).get("last_updated", ""),
        }

        # Extract addresses
        addresses = npi_data.get("addresses", [])
        for addr in addresses:
            if addr.get("address_purpose") == "LOCATION":
                details.update(
                    {
                        "Practice_Address": addr.get("address_1", ""),
                        "Practice_City": addr.get("city", ""),
                        "Practice_State": addr.get("state", ""),
                        "Practice_Zip": addr.get("postal_code", ""),
                        "Practice_Phone": addr.get("telephone_number", ""),
                    }
                )
                break

        # Extract taxonomies (specialties)
        taxonomies = npi_data.get("taxonomies", [])
        if taxonomies:
            primary_taxonomy = next(
                (t for t in taxonomies if t.get("primary", False)),
                taxonomies[0],
            )
            details.update(
                {
                    "Primary_Taxonomy_Code": primary_taxonomy.get("code", ""),
                    "Primary_Taxonomy_Desc": primary_taxonomy.get("desc", ""),
                    "License_Number": primary_taxonomy.get("license", ""),
                    "License_State": primary_taxonomy.get("state", ""),
                }
            )

        return details

    def create_website_columns(self, source):
        """Create website indicator columns"""
        websites = [
            "therapyfinder_therapy",
            "headway_therapy",
            "alma_therapy",
            "rula_therapy",
            "sheet5",
        ]
        website_data = {}

        for website in websites:
            website_data[f"Website_{website}"] = (
                "X" if source and website in str(source).lower() else ""
            )

        return website_data

    def process_npi_records(self):
        """Process all records to check NPI and create enriched data"""
        print("\n" + "=" * 80)
        print("PROCESSING NPI RECORDS")
        print("=" * 80)

        total_processed = 0
        npi_found_count = 0
        npi_not_found_count = 0

        for index, row in self.all_data.iterrows():
            total_processed += 1
            row_dict = row.to_dict()

            # Get ID from different possible fields
            record_id = (
                row_dict.get("ID")
                or row_dict.get("clinician_id")
                or row_dict.get("provider_id")
                or row_dict.get("NPI Number")
                or ""
            )

            print(f"Processing record {total_processed}: ID={record_id}")

            # Fetch NPI data
            npi_data = self.fetch_npi_data(record_id)

            # Create base record with website indicators
            base_record = row_dict.copy()
            website_indicators = self.create_website_columns(
                row_dict.get("Source", "")
            )
            base_record.update(website_indicators)

            if npi_data:
                # NPI Found - add NPI details
                npi_details = self.extract_npi_details(npi_data)
                base_record.update(npi_details)
                base_record["NPI_Status"] = "Found"
                self.npi_found_data.append(base_record)
                npi_found_count += 1
                print(f"  ✓ NPI Found: {record_id}")
            else:
                # NPI Not Found
                base_record["NPI_Status"] = "Not Found"
                self.npi_not_found_data.append(base_record)
                npi_not_found_count += 1
                print(f"  ✗ NPI Not Found: {record_id}")

        # Update analysis data
        self.analysis_data = [
            {"Category": "Total Records Processed", "Count": total_processed},
            {"Category": "NPI Found", "Count": npi_found_count},
            {"Category": "NPI Not Found", "Count": npi_not_found_count},
            {
                "Category": "Success Rate",
                "Count": f"{(npi_found_count/total_processed*100):.1f}%",
            },
            {
                "Category": "Processing Date",
                "Count": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        ]

        print(f"\n✓ Processing Complete:")
        print(f"   - Total Records: {total_processed}")
        print(f"   - NPI Found: {npi_found_count}")
        print(f"   - NPI Not Found: {npi_not_found_count}")

        return total_processed

    def create_cross_tabulation(self, data_list, sheet_name):
        """Create cross-tabulation for Accepted IPs and Main Specialties"""
        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list)

        # Extract unique values from Accepted_IPs and Main_Specialties
        all_ips = set()
        all_specialties = set()

        for record in data_list:
            # Process Accepted IPs
            ips_str = (
                record.get("Accepted_IPs", "")
                or record.get("Accepted IPs", "")
                or ""
            )
            if ips_str and pd.notna(ips_str):
                ips = [
                    ip.strip() for ip in str(ips_str).split(",") if ip.strip()
                ]
                all_ips.update(ips)

            # Process Main Specialties
            specialties_str = (
                record.get("Main_Specialties", "")
                or record.get("Main Specialties", "")
                or ""
            )
            if specialties_str and pd.notna(specialties_str):
                specialties = [
                    spec.strip()
                    for spec in str(specialties_str).split(",")
                    if spec.strip()
                ]
                all_specialties.update(specialties)

        # Create cross-tabulation columns
        cross_tab_data = []

        for record in data_list:
            cross_tab_record = record.copy()

            # Add Accepted IPs cross-tab
            ips_str = (
                record.get("Accepted_IPs", "")
                or record.get("Accepted IPs", "")
                or ""
            )
            current_ips = (
                [ip.strip() for ip in str(ips_str).split(",")]
                if ips_str and pd.notna(ips_str)
                else []
            )

            for ip in all_ips:
                cross_tab_record[f"IP_{ip}"] = "X" if ip in current_ips else ""

            # Add Main Specialties cross-tab
            specialties_str = (
                record.get("Main_Specialties", "")
                or record.get("Main Specialties", "")
                or ""
            )
            current_specialties = (
                [spec.strip() for spec in str(specialties_str).split(",")]
                if specialties_str and pd.notna(specialties_str)
                else []
            )

            for specialty in all_specialties:
                cross_tab_record[f"Specialty_{specialty}"] = (
                    "X" if specialty in current_specialties else ""
                )

            cross_tab_data.append(cross_tab_record)

        return pd.DataFrame(cross_tab_data)

    def export_results(self, output_path):
        """Export all results to Excel"""
        try:
            print(f"\nExporting results to {output_path}...")

            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                # Export NPI Found with cross-tabulation
                if self.npi_found_data:
                    npi_found_df = self.create_cross_tabulation(
                        self.npi_found_data, "NPI Found"
                    )
                    npi_found_df.to_excel(
                        writer, sheet_name="NPI Found", index=False
                    )
                    print(f"✓ NPI Found: {len(npi_found_df)} records")

                # Export NPI Not Found with cross-tabulation
                if self.npi_not_found_data:
                    npi_not_found_df = self.create_cross_tabulation(
                        self.npi_not_found_data, "NPI Not Found"
                    )
                    npi_not_found_df.to_excel(
                        writer, sheet_name="NPI Not Found", index=False
                    )
                    print(f"✓ NPI Not Found: {len(npi_not_found_df)} records")

                # Export Analysis
                if self.analysis_data:
                    analysis_df = pd.DataFrame(self.analysis_data)
                    analysis_df.to_excel(
                        writer, sheet_name="Analysis", index=False
                    )
                    print(f"✓ Analysis: {len(analysis_df)} summary rows")

            print(f"\n✓ Successfully exported all sheets")
            return True

        except Exception as e:
            print(f"Error exporting results: {e}")
            return False


# Configuration
class Config:
    # Set TEST_MODE = False to process all records
    TEST_MODE = False
    # TEST_RECORD_LIMIT = 10


def main():
    # Configuration
    INPUT_FILE = "consolidated_therapy_data_20251025_1941.xlsx"  # Replace with your file path
    OUTPUT_FILE = f"npi_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    # Initialize processor
    processor = NPIAnalysisProcessor(INPUT_FILE)

    # Step 1: Load data (with limit if in test mode)
    limit = Config.TEST_RECORD_LIMIT if Config.TEST_MODE else None
    if not processor.load_data(limit):
        return

    # Step 2: Process NPI records
    total_processed = processor.process_npi_records()

    # Step 3: Export results
    success = processor.export_results(OUTPUT_FILE)

    # Summary
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    if success:
        print(f"✓ Input file: {INPUT_FILE}")
        print(f"✓ Output file: {OUTPUT_FILE}")
        print(f"✓ Total records processed: {total_processed}")
        print(f"✓ Test mode: {Config.TEST_MODE}")
        if Config.TEST_MODE:
            print(f"✓ Record limit: {Config.TEST_RECORD_LIMIT}")

        print(
            "\nTo process all records, set TEST_MODE = False in the Config class"
        )
    else:
        print("❌ Processing failed")


if __name__ == "__main__":
    main()
