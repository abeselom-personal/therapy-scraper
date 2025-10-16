import json
import logging
import os
import pickle
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from pymongo import MongoClient

# Configure beautiful logging
logging.basicConfig(
    level=logging.INFO,
    format="\033[1;36m%(asctime)s\033[0m | \033[1;32m%(levelname)-8s\033[0m | \033[1;33m%(message)s\033[0m",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class AlmaTherapistScraper:
    """
    A comprehensive scraper for Alma therapist data that fetches provider information,
    processes it, stores in MongoDB (or local backup), and exports to Excel format.
    """

    def __init__(
        self,
        mongo_uri: str = "mongodb://localhost:27017/",
        db_name: str = "alma_therapists",
        username: str = None,
        password: str = None,
        use_mongodb: bool = True,
    ):
        """
        Initialize the Alma Therapist Scraper with MongoDB connection or local storage.

        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name for storing therapist data
            username: MongoDB username (optional)
            password: MongoDB password (optional)
            use_mongodb: Whether to use MongoDB or local storage
        """
        logger.info("🏁 Initializing Alma Therapist Scraper...")
        logger.info(
            f"📊 Storage Mode: {'MongoDB' if use_mongodb else 'Local Backup'}"
        )

        self.use_mongodb = use_mongodb
        self.local_data = []
        self.local_backup_file = "alma_therapists_backup.pkl"

        if self.use_mongodb:
            logger.info(f"💾 MongoDB URI: {mongo_uri}")
            logger.info(f"🗄️  Database: {db_name}")

            try:
                if username and password:
                    # Authenticated connection
                    self.client = MongoClient(
                        mongo_uri, serverSelectionTimeoutMS=5000
                    )
                    logger.info("🔐 Using authenticated MongoDB connection")
                else:
                    # Unauthenticated connection
                    self.client = MongoClient(mongo_uri)
                    logger.info("🔓 Using unauthenticated MongoDB connection")

                # Test connection
                self.client.admin.command("ping")
                self.db = self.client[db_name]
                self.collection = self.db["therapists"]
                logger.info("✅ MongoDB connection established successfully")

            except Exception as e:
                logger.error(f"❌ Failed to connect to MongoDB: {e}")
                logger.warning("🔄 Falling back to local storage mode")
                self.use_mongodb = False
                self.local_data = self.load_local_backup()
        else:
            logger.info("💾 Using local storage mode")
            self.local_data = self.load_local_backup()

        self.base_url = "https://secure.helloalma.com"
        self.session = requests.Session()

        # Set headers to mimic a real browser
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://helloalma.com/",
            }
        )

        logger.info("✅ HTTP session configured with proper headers")

    def load_local_backup(self) -> List[Dict]:
        """
        Load data from local backup file if it exists.

        Returns:
            List of previously saved therapist data
        """
        try:
            if os.path.exists(self.local_backup_file):
                with open(self.local_backup_file, "rb") as f:
                    data = pickle.load(f)
                logger.info(f"📂 Loaded {len(data)} records from local backup")
                return data
            else:
                logger.info(
                    "📂 No local backup found, starting with empty dataset"
                )
                return []
        except Exception as e:
            logger.error(f"❌ Error loading local backup: {e}")
            return []

    def save_local_backup(self):
        """Save data to local backup file."""
        try:
            with open(self.local_backup_file, "wb") as f:
                pickle.dump(self.local_data, f)
            logger.info(
                f"💾 Saved {len(self.local_data)} records to local backup"
            )
        except Exception as e:
            logger.error(f"❌ Error saving local backup: {e}")

    def store_data(self, processed_data: Dict) -> bool:
        """
        Store processed data in MongoDB or local storage.

        Args:
            processed_data: Processed therapist data

        Returns:
            True if storage was successful, False otherwise
        """
        try:
            if self.use_mongodb:
                result = self.collection.update_one(
                    {"Sr. NO": processed_data["Sr. NO"]},
                    {"$set": processed_data},
                    upsert=True,
                )

                if result.upserted_id:
                    logger.debug(
                        f"💾 New MongoDB record inserted for {processed_data['Name']}"
                    )
                else:
                    logger.debug(
                        f"🔄 Existing MongoDB record updated for {processed_data['Name']}"
                    )
            else:
                # Local storage - remove existing record if present and add new one
                self.local_data = [
                    data
                    for data in self.local_data
                    if data.get("Sr. NO") != processed_data["Sr. NO"]
                ]
                self.local_data.append(processed_data)
                self.save_local_backup()
                logger.debug(
                    f"💾 Local record stored for {processed_data['Name']}"
                )

            return True

        except Exception as e:
            logger.error(f"❌ Storage failed for {processed_data['Name']}: {e}")
            return False

    def fetch_provider_list(
        self, page: int = 1, limit: int = 15
    ) -> Optional[Dict]:
        """
        Fetch the list of providers from Alma API with pagination.

        Args:
            page: Page number to fetch
            limit: Number of results per page

        Returns:
            JSON response data or None if failed
        """
        url = f"{self.base_url}/api/v1/providerProfiles/search/"
        params = {"page": page, "limit": limit}

        logger.info(f"🌐 Fetching provider list - Page {page}, Limit {limit}")
        logger.debug(f"📡 API URL: {url}")
        logger.debug(f"🔧 Parameters: {params}")

        try:
            start_time = time.time()
            response = self.session.get(url, params=params, timeout=30)
            response_time = time.time() - start_time

            logger.info(
                f"📥 Response received in {response_time:.2f}s - Status: {response.status_code}"
            )

            if response.status_code == 200:
                data = response.json()
                total_count = data.get("count", 0)
                results_count = len(data.get("results", []))
                additional_count = len(data.get("additionalResults", []))

                logger.info(
                    f"📊 Data summary - Total: {total_count:,}, Results: {results_count}, Additional: {additional_count}"
                )
                return data
            else:
                logger.warning(
                    f"⚠️  Non-200 response: {response.status_code} - {response.text[:100]}..."
                )
                return None

        except requests.exceptions.Timeout:
            logger.error("⏰ Request timeout while fetching provider list")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("🔌 Connection error while fetching provider list")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(
                f"❌ Request exception while fetching provider list: {e}"
            )
            return None
        except json.JSONDecodeError as e:
            logger.error(
                f"📄 JSON decode error while fetching provider list: {e}"
            )
            return None

    def fetch_availability(
        self, provider_slug: str, target_date: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Fetch availability data for a specific provider.

        Args:
            provider_slug: Unique identifier for the provider
            target_date: Date to check availability for (YYYY-MM-DD format)

        Returns:
            Availability data or None if failed
        """
        if target_date is None:
            target_date = date.today().isoformat()

        url = f"{self.base_url}/api/v1/providers/{provider_slug}/self_schedule_availability/{target_date}/"

        logger.debug(
            f"📅 Fetching availability for: {provider_slug} on {target_date}"
        )
        logger.debug(f"🔗 Availability URL: {url}")

        try:
            start_time = time.time()
            response = self.session.get(url, timeout=15)
            response_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                available_slots = len(data.get("availableSlots", []))
                next_dates = len(data.get("nextAvailableDates", {}))

                logger.debug(
                    f"✅ Availability fetched in {response_time:.2f}s - Slots: {available_slots}, Next dates: {next_dates}"
                )
                return data
            elif response.status_code == 404:
                logger.debug(
                    f"🔍 No availability data found for {provider_slug}"
                )
                return None
            else:
                logger.debug(
                    f"⚠️  Availability request failed: {response.status_code} for {provider_slug}"
                )
                return None

        except requests.exceptions.Timeout:
            logger.debug(f"⏰ Availability timeout for {provider_slug}")
            return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"❌ Availability error for {provider_slug}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.debug(f"📄 JSON decode error for availability: {e}")
            return None

    def extract_filterables_by_prefix(
        self, filterables: List[Dict], prefix: str
    ) -> str:
        """
        Extract and format filterable items by slug prefix.

        Args:
            filterables: List of filterable items
            prefix: Prefix to filter by

        Returns:
            Comma-separated string of matching items
        """
        items = []
        for item in filterables:
            if item.get("slug", "").startswith(prefix):
                items.append(item.get("name", ""))

        result = ", ".join(items) if items else ""
        logger.debug(f"🔍 Filtered '{prefix}': Found {len(items)} items")
        return result

    def extract_all_specialties(self, filterables: List[Dict]) -> str:
        """
        Extract ALL specialties from filterables for Main Specialties field.

        Args:
            filterables: List of filterable items

        Returns:
            Comma-separated string of ALL specialties
        """
        specialties = []
        for item in filterables:
            if item.get("slug", "").startswith("specialty_v2_"):
                specialties.append(item.get("name", ""))

        all_specialties = ", ".join(specialties) if specialties else ""
        logger.debug(f"🎯 All specialties extracted: {len(specialties)} items")
        return all_specialties

    def extract_treatment_approaches_detailed(
        self, filterables: List[Dict]
    ) -> str:
        """
        Extract treatment approaches in detailed format like the example.

        Args:
            filterables: List of filterable items

        Returns:
            Comma-separated string of treatment approaches
        """
        modalities = []
        for item in filterables:
            if item.get("slug", "").startswith("modality_"):
                modalities.append(item.get("name", ""))

        # Format like the example: "Cognitive Behavioral (CBT), Culturally Sensitive, ..."
        treatment_approaches = ", ".join(modalities) if modalities else ""
        logger.debug(f"🛠️  Treatment approaches: {len(modalities)} items")
        return treatment_approaches

    def extract_appointment_types_detailed(
        self, filterables: List[Dict]
    ) -> str:
        """
        Extract appointment types in detailed format.

        Args:
            filterables: List of filterable items

        Returns:
            Formatted appointment types string
        """
        services = []
        for item in filterables:
            if item.get("slug", "").startswith("service_"):
                service_name = item.get("name", "")
                # Format like "Individual therapy" or "Child and adolescent therapy"
                services.append(service_name)

        # Format like the example: "Video session - 60 minutes"
        # Since we don't have session length, we'll use the services list
        appointment_types = ", ".join(services) if services else "Video session"
        logger.debug(f"📅 Appointment types: {len(services)} items")
        return appointment_types

    def extract_age_groups_detailed(self, filterables: List[Dict]) -> str:
        """
        Extract age groups in detailed format.

        Args:
            filterables: List of filterable items

        Returns:
            Formatted age groups string
        """
        age_groups = []
        for item in filterables:
            if item.get("slug", "").startswith("ages_served_"):
                age_group = item.get("name", "")
                age_groups.append(age_group)

        # Format like the example: "Adults, Individual Therapy, Couples Therapy"
        formatted_ages = ", ".join(age_groups) if age_groups else ""
        logger.debug(f"👥 Age groups: {len(age_groups)} items")
        return formatted_ages

    def extract_highlights(
        self, provider_data: Dict, filterables: List[Dict]
    ) -> str:
        """
        Extract highlights for the provider.

        Args:
            provider_data: Provider data
            filterables: List of filterable items

        Returns:
            Formatted highlights string
        """
        highlights = []

        # Add states
        licensure_states = provider_data.get("licensureStates", [])
        if licensure_states:
            highlights.append(", ".join(licensure_states))

        # Add verification status
        highlights.append("Verified by Alma")

        # Add service types
        services = []
        for item in filterables:
            if item.get("slug", "").startswith("service_"):
                services.append(item.get("name", ""))
        if services:
            highlights.append(", ".join(services))

        # Add insurance status
        accepted_insurance = provider_data.get("acceptedInsuranceSlugs", [])
        if accepted_insurance:
            highlights.append("Accepts your insurance")

        highlights_str = ", ".join(highlights)
        logger.debug(f"⭐ Highlights: {len(highlights)} items")
        return highlights_str

    def extract_insurance_names_detailed(
        self, insurance_slugs: List[str]
    ) -> str:
        """
        Extract and format insurance names in detailed format.

        Args:
            insurance_slugs: List of insurance slugs

        Returns:
            Comma-separated string of formatted insurance names
        """
        insurance_names = []
        for slug in insurance_slugs:
            # Remove 'payment_' prefix and replace underscores with spaces
            name = slug.replace("payment_", "").replace("_", " ").title()
            # Clean up common insurance names
            if "aetna" in name.lower():
                name = "Aetna"
            elif "cigna" in name.lower():
                name = "Cigna"
            elif "united" in name.lower() and "health" in name.lower():
                name = "United Healthcare"
            elif "oxford" in name.lower():
                name = "Oxford Health Plans"
            elif "optum" in name.lower():
                name = "Optum"
            insurance_names.append(name)

        # Remove duplicates and sort
        unique_insurance = sorted(list(set(insurance_names)))
        result = ", ".join(unique_insurance)
        logger.debug(
            f"🏥 Insurance names extracted: {len(unique_insurance)} providers"
        )
        return result

    def parse_rate_range(self, rate_value: str) -> tuple:
        """
        Parse min and max session price from rate value string.

        Args:
            rate_value: Rate string like "$200-260" or "$140"

        Returns:
            Tuple of (min_price, max_price) as strings
        """
        if not rate_value:
            logger.debug("💰 No rate value provided")
            return "", ""

        try:
            rate_clean = rate_value.replace("$", "").strip()
            if "-" in rate_clean:
                min_price, max_price = rate_clean.split("-")
                min_price = min_price.strip()
                max_price = max_price.strip()
                logger.debug(f"💰 Rate range parsed: ${min_price}-${max_price}")
                return min_price, max_price
            else:
                logger.debug(f"💰 Single rate parsed: ${rate_clean}")
                return rate_clean, rate_clean
        except Exception as e:
            logger.warning(f"⚠️  Failed to parse rate value '{rate_value}': {e}")
            return "", ""

    def calculate_total_slots_7_days(
        self, availability_data: Optional[Dict]
    ) -> int:
        """
        Calculate total number of available slots in the next 7 days.

        Args:
            availability_data: Availability data for the provider

        Returns:
            Total number of slots in next 7 days
        """
        if (
            not availability_data
            or "nextAvailableDates" not in availability_data
        ):
            return 0

        next_dates = availability_data["nextAvailableDates"]
        total_slots = 0

        # Get today's date for comparison
        today = datetime.now().date()

        for date_str, slots in next_dates.items():
            try:
                # Parse the date string
                date_obj = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).date()

                # Check if the date is within the next 7 days
                days_difference = (date_obj - today).days
                if (
                    0 <= days_difference <= 6
                ):  # Today + next 6 days = 7 days total
                    total_slots += len(slots)

            except Exception as e:
                logger.debug(
                    f"⚠️  Error processing date {date_str} for slot count: {e}"
                )
                continue

        logger.debug(f"📊 Total slots in 7 days: {total_slots}")
        return total_slots

    def generate_booking_summary(
        self, availability_data: Optional[Dict]
    ) -> str:
        """
        Generate detailed booking summary from availability data.

        Args:
            availability_data: Availability data for the provider

        Returns:
            Formatted booking summary string
        """
        if (
            not availability_data
            or "nextAvailableDates" not in availability_data
        ):
            return ""

        next_dates = availability_data["nextAvailableDates"]
        booking_parts = []

        # Sort dates and take next 21 days
        sorted_dates = sorted(next_dates.keys())[:21]

        for date_str in sorted_dates:
            try:
                date_obj = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                )
                formatted_date = date_obj.strftime("%a - %b %d")
                slots = next_dates[date_str]
                slot_count = len(slots)
                slot_text = "slot" if slot_count == 1 else "slots"
                booking_parts.append(
                    f"{formatted_date}: {slot_count} {slot_text} (60 min)"
                )
            except Exception as e:
                logger.debug(f"⚠️  Error formatting date {date_str}: {e}")
                continue

        booking_summary = "; ".join(booking_parts)
        logger.debug(
            f"📅 Booking summary generated: {len(booking_parts)} dates"
        )
        return booking_summary

    def process_provider_data(
        self, provider_data: Dict, availability_data: Optional[Dict] = None
    ) -> Dict:
        """
        Process raw provider data into structured format for Excel export.

        Args:
            provider_data: Raw provider data from API
            availability_data: Availability data for the provider

        Returns:
            Processed data dictionary
        """
        provider_id = provider_data.get("providerId", "Unknown")
        provider_slug = provider_data.get("providerSlug", "")

        logger.info(f"🔧 Processing provider: {provider_id} - {provider_slug}")

        # Basic info
        profile_url = (
            f"https://helloalma.com/providers/{provider_slug}/"
            if provider_slug
            else ""
        )

        # Name - format as uppercase like the example
        first_name = provider_data.get("providerFirstName", "")
        last_name = provider_data.get("providerLastName", "")
        full_name = f"{first_name} {last_name}".strip().upper()

        logger.debug(f"👤 Provider name: {full_name}")

        # Profession and bio
        profession = provider_data.get("title", "")
        bio = provider_data.get("summary", "")
        logger.debug(f"🎓 Profession: {profession}")

        # Filterables extraction
        filterables = provider_data.get("filterables", [])
        logger.debug(f"🏷️  Total filterables: {len(filterables)}")

        # Enhanced field extraction for the desired format
        treatment_approaches = self.extract_treatment_approaches_detailed(
            filterables
        )
        appointment_types = self.extract_appointment_types_detailed(filterables)
        age_groups = self.extract_age_groups_detailed(filterables)
        highlights = self.extract_highlights(provider_data, filterables)
        all_specialties = self.extract_all_specialties(filterables)

        # Languages
        languages = self.extract_filterables_by_prefix(filterables, "language_")

        # Gender
        gender = self.extract_filterables_by_prefix(
            filterables, "identity_gender_"
        )

        # Race/Ethnicity
        race_ethnicity = self.extract_filterables_by_prefix(
            filterables, "identity_race_"
        )

        # Licenses and states
        licensure_states = provider_data.get("licensureStates", [])
        licenses = ", ".join(licensure_states) if licensure_states else ""
        logger.debug(f"📜 Licenses: {licenses}")

        # Rate parsing
        rate_value = provider_data.get("rateValue", "")
        min_price, max_price = self.parse_rate_range(rate_value)

        # Payment options - enhanced formatting
        accepted_insurance = provider_data.get("acceptedInsuranceSlugs", [])
        verified_insurance = provider_data.get(
            "verifiedAcceptedInsuranceSlugs", []
        )
        all_insurance = list(set(accepted_insurance + verified_insurance))
        insurance_names = self.extract_insurance_names_detailed(all_insurance)
        logger.debug(f"💳 Insurance providers: {len(all_insurance)}")

        # Pay out of pocket status
        pay_out_of_pocket = (
            "Yes"
            if any(
                item.get("slug") == "payment_out_of_pocket"
                for item in filterables
            )
            else "No"
        )
        logger.debug(f"💵 Out of pocket: {pay_out_of_pocket}")

        # Enhanced booking summary
        booking_summary = self.generate_booking_summary(availability_data)

        # NEW: Calculate total slots in 7 days
        total_slots_7_days = self.calculate_total_slots_7_days(
            availability_data
        )

        # Construct the complete data row matching the exact format
        processed_data = {
            "Url": f"https://secure.helloalma.com/providers/{provider_slug}/",
            "Name": full_name,  # Uppercase like example
            "Profession": profession,
            "Clinic Name": "",  # Not available in API
            "Bio": bio,
            "Additional Focus Areas": "",  # Not separating additional focus areas anymore
            "Treatment Approaches": treatment_approaches,
            "Appointment Types": appointment_types,
            "Communities": "",  # Not using communities field
            "Age Groups": age_groups,
            "Languages": languages,
            "Highlights": highlights,
            "Gender": gender,
            "Pronouns": "",  # Not available in API
            "Race Ethnicity": race_ethnicity,
            "Licenses": f"Licensed {profession.split(', ')[-1] if ',' in profession else profession}",
            "Locations": "Video session: Online",  # Default to online since Alma is primarily virtual
            "Education": "",  # Not available in API
            "Faiths": "",  # Not available in API
            "Min Session Price": min_price,
            "Max Session Price": max_price,
            "Pay Out Of Pocket Status": pay_out_of_pocket,
            "Individual Service Rates": rate_value,
            "General Payment Options": insurance_names,
            "Booking Summary": booking_summary,
            "Booking Url": profile_url,
            "Listed In States": licenses,
            "States": licenses,
            "Listed In Websites": "Hello Alma",
            "Urls": profile_url,
            "Connect Link - Facebook": "",
            "Connect Link - Instagram": "",
            "Connect Link - LinkedIn": "",
            "Connect Link - Twitter": "",
            "Connect Link - Website": "",
            "Main Specialties": all_specialties,  # Now contains ALL specialties
            "Accepted IPs": insurance_names,
            "Total Slots in 7 Days": total_slots_7_days,  # NEW COLUMN
            "Sr. NO": provider_data.get("providerId", ""),
            "raw_data": provider_data,  # Store raw data for reference
            "scraped_at": datetime.now(),
            "processed_at": datetime.now().isoformat(),
        }
        print(processed_data)
        logger.info(
            f"✅ Successfully processed: {full_name} (ID: {provider_id}) - {total_slots_7_days} slots in 7 days"
        )
        return processed_data

    def scrape_and_store(self, pages: int = 1, limit: int = 15) -> List[Dict]:
        """
        Main method to scrape data from multiple pages and store in MongoDB or locally.

        Args:
            pages: Number of pages to scrape
            limit: Number of results per page

        Returns:
            List of all processed data
        """
        all_processed_data = []
        total_providers_processed = 0
        total_pages_processed = 0
        successful_storages = 0

        logger.info(
            f"🚀 Starting scraping process - Pages: {pages}, Limit: {limit}"
        )
        logger.info("=" * 80)

        for page in range(1, pages + 1):
            logger.info(f"📄 Processing page {page}/{pages}...")

            # Fetch provider list
            provider_data = self.fetch_provider_list(page=page, limit=limit)
            if not provider_data:
                logger.warning(f"⚠️  Skipping page {page} due to fetch failure")
                continue

            # Process both main results and additional results
            all_providers = provider_data.get(
                "results", []
            ) + provider_data.get("additionalResults", [])
            logger.info(
                f"👥 Found {len(all_providers)} providers on page {page}"
            )

            page_processed_count = 0
            page_successful_storages = 0

            for provider in all_providers:
                try:
                    provider_id = provider.get("providerId", "Unknown")
                    provider_slug = provider.get("providerSlug", "Unknown")

                    logger.info(
                        f"👤 Processing provider: {provider_id} - {provider_slug}"
                    )

                    # Fetch availability data
                    availability_data = None
                    if provider_slug and provider_slug != "Unknown":
                        logger.debug(
                            f"📅 Fetching availability for {provider_slug}"
                        )
                        availability_data = self.fetch_availability(
                            provider_slug
                        )
                        time.sleep(0.5)  # Be respectful to the server
                    else:
                        logger.warning(
                            f"⚠️  No provider slug for ID {provider_id}, skipping availability"
                        )

                    # Process the data
                    processed_data = self.process_provider_data(
                        provider, availability_data
                    )

                    # Store the data
                    storage_success = self.store_data(processed_data)

                    if storage_success:
                        page_successful_storages += 1
                        successful_storages += 1
                        logger.info(
                            f"💾 Storage successful: {processed_data['Name']}"
                        )
                    else:
                        logger.warning(
                            f"⚠️  Storage failed but data processed: {processed_data['Name']}"
                        )

                    all_processed_data.append(processed_data)
                    page_processed_count += 1
                    total_providers_processed += 1

                except Exception as e:
                    logger.error(
                        f"❌ Error processing provider {provider.get('providerSlug', 'unknown')}: {e}"
                    )
                    continue

            total_pages_processed += 1
            logger.info(
                f"📊 Page {page} completed: {page_processed_count}/{len(all_providers)} providers processed, {page_successful_storages} stored successfully"
            )

            # Add delay between pages to be respectful
            if page < pages:
                delay = 2
                logger.info(f"⏳ Waiting {delay} seconds before next page...")
                time.sleep(delay)

        logger.info("=" * 80)
        logger.info(f"🎉 Scraping completed successfully!")
        logger.info(f"📈 Total pages processed: {total_pages_processed}")
        logger.info(
            f"👥 Total providers processed: {total_providers_processed}"
        )
        logger.info(
            f"💾 Total records successfully stored: {successful_storages}"
        )
        logger.info(f"📋 Total records in memory: {len(all_processed_data)}")

        return all_processed_data

    def export_to_excel(
        self, filename: str = "alma_therapists.xlsx"
    ) -> pd.DataFrame:
        """
        Export data from storage to Excel with specified format.

        Args:
            filename: Output Excel filename

        Returns:
            DataFrame containing the exported data
        """
        logger.info(f"💾 Exporting data to Excel: {filename}")

        try:
            # Get data from appropriate source
            if self.use_mongodb:
                # Fetch all documents from MongoDB, excluding the raw_data field
                cursor = self.collection.find(
                    {},
                    {
                        "raw_data": 0,
                        "scraped_at": 0,
                        "_id": 0,
                        "processed_at": 0,
                    },
                )
                data_list = list(cursor)
                total_documents = self.collection.count_documents({})
                logger.info(f"📊 Found {total_documents} documents in MongoDB")
            else:
                # Use local data, remove internal fields
                data_list = []
                for item in self.local_data:
                    cleaned_item = {
                        k: v
                        for k, v in item.items()
                        if k not in ["raw_data", "scraped_at", "processed_at"]
                    }
                    data_list.append(cleaned_item)
                total_documents = len(data_list)
                logger.info(
                    f"📊 Found {total_documents} documents in local storage"
                )

            # Convert to DataFrame
            df = pd.DataFrame(data_list)

            if df.empty:
                logger.warning("⚠️  No data found to export")
                return pd.DataFrame()

            logger.info(
                f"📋 DataFrame created with {len(df)} rows and {len(df.columns)} columns"
            )

            # Ensure the column order matches the specified header EXACTLY with the new column
            expected_columns = [
                "Url",
                "Name",
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
                "Total Slots in 7 Days",  # NEW COLUMN
                "Sr. NO",
            ]

            logger.info(f"📑 Expected columns: {len(expected_columns)}")
            logger.info(f"📑 Actual columns: {len(df.columns)}")

            # Add missing columns with empty values
            missing_columns = []
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = ""
                    missing_columns.append(col)

            if missing_columns:
                logger.info(f"📝 Added missing columns: {len(missing_columns)}")
                logger.debug(f"📝 Missing columns: {missing_columns}")

            # Reorder columns to match exact specification
            df = df[expected_columns]
            logger.info("✅ Columns reordered to match expected format")

            # Calculate statistics for the new column
            if not df.empty and "Total Slots in 7 Days" in df.columns:
                total_slots_series = pd.to_numeric(
                    df["Total Slots in 7 Days"], errors="coerce"
                )
                avg_slots = total_slots_series.mean()
                max_slots = total_slots_series.max()
                providers_with_slots = total_slots_series[
                    total_slots_series > 0
                ].count()

                logger.info("📊 Availability Statistics:")
                logger.info(f"   • Average slots in 7 days: {avg_slots:.1f}")
                logger.info(f"   • Maximum slots in 7 days: {max_slots}")
                logger.info(
                    f"   • Providers with available slots: {providers_with_slots}/{len(df)}"
                )

            # Export to Excel
            try:
                df.to_excel(filename, index=False, engine="openpyxl")
                logger.info(f"✅ Excel file created successfully: {filename}")

                # Print summary statistics
                logger.info("📊 Export Summary:")
                logger.info(f"   • Total therapists: {len(df)}")
                logger.info(f"   • Total columns: {len(df.columns)}")
                logger.info(
                    f"   • File size: {len(df) * len(df.columns)} data points"
                )

                # Show some basic stats about the data
                if not df.empty:
                    states_count = (
                        df["States"].str.split(", ").explode().nunique()
                    )
                    specialties_count = (
                        df["Main Specialties"]
                        .str.split(", ")
                        .explode()
                        .nunique()
                    )
                    logger.info(f"   • Unique states: {states_count}")
                    logger.info(f"   • Unique specialties: {specialties_count}")

            except Exception as e:
                logger.error(f"❌ Failed to create Excel file: {e}")
                raise

            return df

        except Exception as e:
            logger.error(f"❌ Error during Excel export: {e}")
            raise

    def get_scraping_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the scraping process and stored data.

        Returns:
            Dictionary containing scraping statistics
        """
        try:
            if self.use_mongodb:
                total_documents = self.collection.count_documents({})
                latest_scrape = self.collection.find_one(
                    sort=[("scraped_at", -1)]
                )
                storage_type = "MongoDB"
            else:
                total_documents = len(self.local_data)
                latest_scrape = (
                    max(
                        self.local_data,
                        key=lambda x: x.get("scraped_at", datetime.min),
                    )
                    if self.local_data
                    else None
                )
                storage_type = "Local Storage"

            stats = {
                "storage_type": storage_type,
                "total_therapists": total_documents,
                "last_scraped": (
                    latest_scrape.get("scraped_at") if latest_scrape else None
                ),
            }

            logger.info("📈 Scraping Statistics:")
            logger.info(f"   • Storage type: {stats['storage_type']}")
            logger.info(f"   • Total therapists: {stats['total_therapists']}")
            logger.info(f"   • Last scraped: {stats['last_scraped']}")

            return stats

        except Exception as e:
            logger.error(f"❌ Error getting statistics: {e}")
            return {}

    def close(self):
        """Close connections and cleanup resources."""
        logger.info("🔚 Closing Alma Therapist Scraper...")
        try:
            if self.use_mongodb:
                self.client.close()
            self.session.close()
            logger.info("✅ Resources cleaned up successfully")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


def main():
    """
    Main execution function for the Alma Therapist Scraper.
    """
    logger.info("🎬 Starting Alma Therapist Data Scraper")
    logger.info("=" * 80)

    # Configuration - Set these according to your MongoDB setup
    # MONGO_URI = "mongodb://localhost:27017/"
    MONGO_URI = "mongodb://scraper:scraper@localhost:27017/helloalma_speed_test?authSource=admin"
    DB_NAME = "alma_therapists"
    MONGO_USERNAME = "scraper"
    MONGO_PASSWORD = "scraper"
    USE_MONGODB = True

    # Initialize the scraper
    scraper = AlmaTherapistScraper(
        mongo_uri=MONGO_URI,
        db_name=DB_NAME,
        username=MONGO_USERNAME,
        password=MONGO_PASSWORD,
        use_mongodb=USE_MONGODB,
    )

    try:
        # Display startup information
        logger.info("🚀 Configuration:")
        logger.info(f"   • Target: Hello Alma Therapist Directory")
        logger.info(
            f"   • Storage: {'MongoDB' if USE_MONGODB else 'Local Backup'}"
        )
        logger.info(f"   • Output: Structured therapist data")

        logger.info("🌐 Beginning data scraping process...")
        for i in range(2388):
            scraper.scrape_and_store(pages=i + 1, limit=15)

        logger.info("📊 Generating scraping statistics...")
        scraper.get_scraping_statistics()

        # Export to Excel
        logger.info("💾 Beginning Excel export process...")
        df = scraper.export_to_excel("alma_therapists_export.xlsx")

        if not df.empty:
            logger.info("🎉 Process completed successfully!")
            logger.info(f"📁 Output file: alma_therapists_export.xlsx")
            logger.info(f"📊 Final count: {len(df)} therapists exported")
        else:
            logger.warning("⚠️  Process completed but no data was exported")

    except Exception as e:
        logger.error(f"💥 Fatal error in main process: {e}")

    finally:
        # Always close resources
        scraper.close()
        logger.info("=" * 80)
        logger.info("🏁 Alma Therapist Scraper has finished execution")


if __name__ == "__main__":
    main()
