import json
import logging
import os
import pickle
import sys
import time
import uuid
from collections import Counter
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
        mongo_uri: str = None,
        db_name: str = None,
        username: str = None,
        password: str = None,
        use_mongodb: bool = True,
    ):
        """
        Initialize the Alma Therapist Scraper with MongoDB connection or local storage.
        """
        logger.info("🏁 Initializing Alma Therapist Scraper...")

        # Use environment variables if not provided
        if mongo_uri is None:
            mongo_host = os.getenv("MONGO_HOST", "mongodb")
            mongo_port = os.getenv("MONGO_PORT", "27017")
            mongo_user = os.getenv("MONGO_USER", "scraper")
            mongo_password = os.getenv("MONGO_PASSWORD", "scraper")
            mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"

        if db_name is None:
            db_name = os.getenv("MONGO_DB", "alma_scraper_final")

        logger.info(
            f"📊 Storage Mode: {'MongoDB' if use_mongodb else 'Local Backup'}"
        )
        logger.info(f"💾 MongoDB URI: {mongo_uri}")
        logger.info(f"🗄️  Database: {db_name}")

        self.use_mongodb = use_mongodb
        self.local_data = []
        self.local_backup_file = "alma_therapists_backup.pkl"

        if self.use_mongodb:
            try:
                self.client = MongoClient(
                    mongo_uri, serverSelectionTimeoutMS=5000
                )
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
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://helloalma.com/",
            }
        )

        # NPI API configuration
        self.npi_base_url = "https://npiregistry.cms.hhs.gov/api/"
        self.npi_headers = {"User-Agent": "NPI-Lookup/1.0"}

        logger.info("✅ HTTP session configured with proper headers")

    def find_npi(self, data: Dict) -> Optional[Dict]:
        """
        Enhanced NPI lookup with multiple fallback strategies and comprehensive data extraction.

        Args:
            data: Provider data containing name, location, and license information

        Returns:
            Dictionary containing NPI data or None if not found
        """
        print(data)
        first_name = data.get("first_name", "").strip()
        last_name = data.get("last_name", "").strip()
        licensure_states = data.get("licensureStates", [])
        print(first_name)
        print(last_name)
        print(licensure_states)

        if not first_name or not last_name:
            logger.warning("⚠️  Missing first or last name for NPI lookup")
            return None

        logger.info(f"🔍 Searching NPI for: {first_name} {last_name}")

        # Strategy 1: Try with state if available
        if licensure_states:
            for state in licensure_states[:3]:  # Try first 3 states
                npi_data = self._npi_lookup_with_state(
                    first_name, last_name, state
                )
                if npi_data:
                    logger.info(
                        f"✅ NPI found using state {state}: {npi_data.get('npi_number')}"
                    )
                    return npi_data

        # Strategy 2: Try without state
        npi_data = self._npi_lookup_basic(first_name, last_name)
        if npi_data:
            logger.info(
                f"✅ NPI found using basic search: {npi_data.get('npi_number')}"
            )
            return npi_data

        # Strategy 3: Try with name variations
        npi_data = self._npi_lookup_variations(first_name, last_name)
        if npi_data:
            logger.info(
                f"✅ NPI found using name variations: {npi_data.get('npi_number')}"
            )
            return npi_data

        logger.warning(f"⚠️  No NPI found for {first_name} {last_name}")
        return None

    def _npi_lookup_with_state(
        self, first_name: str, last_name: str, state: str
    ) -> Optional[Dict]:
        """NPI lookup with state filter."""
        params = {
            "first_name": first_name,
            "last_name": last_name,
            "state": state,
            "limit": 5,  # Get multiple results to find best match
            "version": "2.1",
        }

        try:
            response = requests.get(
                self.npi_base_url,
                headers=self.npi_headers,
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("result_count", 0) > 0:
                return self._extract_npi_data(
                    data["results"][0], "state_search"
                )

        except Exception as e:
            logger.debug(f"❌ State-based NPI lookup failed for {state}: {e}")

        return None

    def _npi_lookup_basic(
        self, first_name: str, last_name: str
    ) -> Optional[Dict]:
        """Basic NPI lookup without state filter."""
        params = {
            "first_name": first_name,
            "last_name": last_name,
            "limit": 5,
            "version": "2.1",
        }

        try:
            response = requests.get(
                self.npi_base_url,
                headers=self.npi_headers,
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("result_count", 0) > 0:
                return self._extract_npi_data(
                    data["results"][0], "basic_search"
                )

        except Exception as e:
            logger.debug(f"❌ Basic NPI lookup failed: {e}")

        return None

    def _npi_lookup_variations(
        self, first_name: str, last_name: str
    ) -> Optional[Dict]:
        """NPI lookup with name variations."""
        variations = [
            (first_name, last_name),  # Original
            (first_name.upper(), last_name.upper()),  # Uppercase
            (
                first_name,
                last_name.split()[0] if " " in last_name else last_name,
            ),  # Handle compound last names
        ]

        for first_var, last_var in variations:
            if first_var != first_name or last_var != last_name:
                params = {
                    "first_name": first_var,
                    "last_name": last_var,
                    "limit": 3,
                    "version": "2.1",
                }

                try:
                    response = requests.get(
                        self.npi_base_url,
                        headers=self.npi_headers,
                        params=params,
                        timeout=10,
                    )
                    response.raise_for_status()
                    data = response.json()

                    if data.get("result_count", 0) > 0:
                        npi_data = self._extract_npi_data(
                            data["results"][0], "variation_search"
                        )
                        if npi_data:
                            return npi_data

                except Exception as e:
                    logger.debug(f"❌ Variation NPI lookup failed: {e}")
                    continue

        return None

    def _extract_npi_data(self, npi_result: Dict, search_type: str) -> Dict:
        """
        Extract comprehensive data from NPI API response.

        Args:
            npi_result: Raw NPI API result
            search_type: Type of search that found this result

        Returns:
            Dictionary with extracted NPI data
        """
        try:
            basic_info = npi_result.get("basic", {})
            addresses = npi_result.get("addresses", [])
            taxonomies = npi_result.get("taxonomies", [])
            identifiers = npi_result.get("identifiers", [])

            # Extract primary taxonomy
            primary_taxonomy = None
            other_taxonomies = []
            for taxonomy in taxonomies:
                if taxonomy.get("primary", False):
                    primary_taxonomy = taxonomy
                else:
                    other_taxonomies.append(taxonomy)

            # Extract addresses by purpose
            mailing_address = None
            practice_address = None
            for address in addresses:
                if address.get("address_purpose") == "MAILING":
                    mailing_address = address
                elif address.get("address_purpose") == "LOCATION":
                    practice_address = address

            npi_data = {
                "npi_number": npi_result.get("number"),
                "search_type": search_type,
                "enumeration_type": npi_result.get("enumeration_type"),
                "basic_info": {
                    "first_name": basic_info.get("first_name"),
                    "last_name": basic_info.get("last_name"),
                    "middle_name": basic_info.get("middle_name"),
                    "credential": basic_info.get("credential"),
                    "sole_proprietor": basic_info.get("sole_proprietor"),
                    "gender": basic_info.get("gender"),
                    "enumeration_date": basic_info.get("enumeration_date"),
                    "last_updated": basic_info.get("last_updated"),
                    "status": basic_info.get("status"),
                    "name_prefix": basic_info.get("name_prefix"),
                    "name_suffix": basic_info.get("name_suffix"),
                },
                "primary_taxonomy": (
                    {
                        "code": (
                            primary_taxonomy.get("code")
                            if primary_taxonomy
                            else None
                        ),
                        "description": (
                            primary_taxonomy.get("desc")
                            if primary_taxonomy
                            else None
                        ),
                        "license_number": (
                            primary_taxonomy.get("license")
                            if primary_taxonomy
                            else None
                        ),
                        "state": (
                            primary_taxonomy.get("state")
                            if primary_taxonomy
                            else None
                        ),
                        "primary": (
                            primary_taxonomy.get("primary")
                            if primary_taxonomy
                            else None
                        ),
                    }
                    if primary_taxonomy
                    else None
                ),
                "other_taxonomies": [
                    {
                        "code": tax.get("code"),
                        "description": tax.get("desc"),
                        "license_number": tax.get("license"),
                        "state": tax.get("state"),
                        "primary": tax.get("primary"),
                    }
                    for tax in other_taxonomies
                ],
                "mailing_address": (
                    {
                        "address_1": (
                            mailing_address.get("address_1")
                            if mailing_address
                            else None
                        ),
                        "address_2": (
                            mailing_address.get("address_2")
                            if mailing_address
                            else None
                        ),
                        "city": (
                            mailing_address.get("city")
                            if mailing_address
                            else None
                        ),
                        "state": (
                            mailing_address.get("state")
                            if mailing_address
                            else None
                        ),
                        "postal_code": (
                            mailing_address.get("postal_code")
                            if mailing_address
                            else None
                        ),
                        "country": (
                            mailing_address.get("country_name")
                            if mailing_address
                            else None
                        ),
                        "phone": (
                            mailing_address.get("telephone_number")
                            if mailing_address
                            else None
                        ),
                        "fax": (
                            mailing_address.get("fax_number")
                            if mailing_address
                            else None
                        ),
                    }
                    if mailing_address
                    else None
                ),
                "practice_address": (
                    {
                        "address_1": (
                            practice_address.get("address_1")
                            if practice_address
                            else None
                        ),
                        "address_2": (
                            practice_address.get("address_2")
                            if practice_address
                            else None
                        ),
                        "city": (
                            practice_address.get("city")
                            if practice_address
                            else None
                        ),
                        "state": (
                            practice_address.get("state")
                            if practice_address
                            else None
                        ),
                        "postal_code": (
                            practice_address.get("postal_code")
                            if practice_address
                            else None
                        ),
                        "country": (
                            practice_address.get("country_name")
                            if practice_address
                            else None
                        ),
                        "phone": (
                            practice_address.get("telephone_number")
                            if practice_address
                            else None
                        ),
                        "fax": (
                            practice_address.get("fax_number")
                            if practice_address
                            else None
                        ),
                    }
                    if practice_address
                    else None
                ),
                "identifiers": [
                    {
                        "code": ident.get("code"),
                        "description": ident.get("desc"),
                        "issuer": ident.get("issuer"),
                        "identifier": ident.get("identifier"),
                        "state": ident.get("state"),
                    }
                    for ident in identifiers
                ],
                "raw_npi_response": npi_result,  # Keep original for reference
            }

            return npi_data

        except Exception as e:
            logger.error(f"❌ Error extracting NPI data: {e}")
            return None

    def load_local_backup(self) -> List[Dict]:
        """Load data from local backup file if it exists."""
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
        """Store processed data in MongoDB or local storage."""
        try:
            if self.use_mongodb:
                result = self.collection.update_one(
                    {"provider_id": processed_data["provider_id"]},
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
                self.local_data = [
                    data
                    for data in self.local_data
                    if data.get("provider_id") != processed_data["provider_id"]
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
        """Fetch the list of providers from Alma API with pagination."""
        url = f"{self.base_url}/api/v1/providerProfiles/search/"
        params = {"page": page, "limit": limit}

        logger.info(f"🌐 Fetching provider list - Page {page}, Limit {limit}")

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
                logger.warning(f"⚠️  Non-200 response: {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            logger.error("⏰ Request timeout while fetching provider list")
            return None
        except Exception as e:
            logger.error(
                f"❌ Request exception while fetching provider list: {e}"
            )
            return None

    def extract_all_provider_data(self, api_data: Dict) -> List[Dict]:
        """Extract all provider data from the API response."""
        all_providers = []

        api_metadata = {
            "total_count": api_data.get("count", 0),
            "additional_results_count": api_data.get(
                "additionalResultsCount", 0
            ),
        }

        logger.info(
            f"📊 API Metadata: Total Count: {api_metadata['total_count']:,}"
        )

        # Process main results
        main_results = api_data.get("results", [])
        logger.info(f"🔍 Processing {len(main_results)} main results...")

        for index, provider in enumerate(main_results, 1):
            provider_data = self.extract_provider_details(
                provider, index, "main"
            )
            all_providers.append(provider_data)

        # Process additional results
        additional_results = api_data.get("additionalResults", [])
        logger.info(
            f"🔍 Processing {len(additional_results)} additional results..."
        )

        for index, provider in enumerate(additional_results, 1):
            provider_data = self.extract_provider_details(
                provider, index, "additional"
            )
            all_providers.append(provider_data)

        logger.info(f"🎯 Total Providers Processed: {len(all_providers)}")
        return all_providers

    def extract_provider_details(
        self, provider: Dict, index: int, source: str
    ) -> Dict:
        """Extract complete details from a single provider object."""
        provider_id = provider.get("providerId", f"unknown_{index}")

        # Basic provider information
        basic_info = {
            "source": source,
            "provider_id": provider_id,
            "profile_id": provider.get("providerProfileId", ""),
            "slug": provider.get("providerSlug", ""),
            "name": f"{provider.get('providerFirstName', '')} {provider.get('providerLastName', '')}".strip(),
            "first_name": provider.get("providerFirstName", ""),
            "last_name": provider.get("providerLastName", ""),
            "title": provider.get("title", ""),
            "summary": provider.get("summary", ""),
            "profile_photo": provider.get("profilePhoto", ""),
            "rate_value": provider.get("rateValue", ""),
            "has_video": provider.get("hasVideo", False),
            "licensure_states": provider.get("licensureStates", []),
            "accepted_insurance_slugs": provider.get(
                "acceptedInsuranceSlugs", []
            ),
        }

        # Extract and categorize filterables
        filterables = provider.get("filterables", [])
        categorized_filterables = self.categorize_filterables(filterables)

        complete_provider_data = {
            **basic_info,
            "filterables": filterables,
            "categorized_filterables": categorized_filterables,
            "raw_data": provider,
        }

        return complete_provider_data

    def categorize_filterables(
        self, filterables: List[Dict]
    ) -> Dict[str, List[str]]:
        """Categorize filterables into meaningful groups."""
        categories = {
            "rates": [],
            "degrees": [],
            "genders": [],
            "services": [],
            "payment_methods": [],
            "race_ethnicity": [],
            "teletherapy_preferences": [],
            "sexuality": [],
            "licensure": [],
            "specialties": [],
            "ages_served": [],
            "modalities": [],
            "faiths": [],
            "session_availability": [],
            "insurance_providers": [],
            "languages": [],
        }

        for item in filterables:
            slug = item.get("slug", "")
            name = item.get("name", "")

            if slug.startswith("rate_"):
                categories["rates"].append(name)
            elif slug.startswith("degree_"):
                categories["degrees"].append(name)
            elif slug.startswith("identity_gender_"):
                categories["genders"].append(name)
            elif slug.startswith("service_"):
                categories["services"].append(name)
            elif slug.startswith("payment_"):
                if "out_of_pocket" in slug or "sliding_scale" in slug:
                    categories["payment_methods"].append(name)
                else:
                    categories["insurance_providers"].append(name)
            elif slug.startswith("identity_race_"):
                categories["race_ethnicity"].append(name)
            elif slug.startswith("teletherapy_preference_"):
                categories["teletherapy_preferences"].append(name)
            elif slug.startswith("identity_sexuality_"):
                categories["sexuality"].append(name)
            elif slug.startswith("licensure_"):
                categories["licensure"].append(name)
            elif slug.startswith("specialty_v2_"):
                categories["specialties"].append(name)
            elif slug.startswith("ages_served_"):
                categories["ages_served"].append(name)
            elif slug.startswith("modality_"):
                categories["modalities"].append(name)
            elif slug.startswith("identity_faith_"):
                categories["faiths"].append(name)
            elif slug.startswith("session_availability_"):
                categories["session_availability"].append(name)
            elif slug.startswith("language_"):
                categories["languages"].append(name)

        return categories

    def extract_filterables_by_prefix(
        self, filterables: List[Dict], prefix: str
    ) -> str:
        """Extract and format filterable items by slug prefix."""
        items = []
        for item in filterables:
            if item.get("slug", "").startswith(prefix):
                items.append(item.get("name", ""))
        return ", ".join(items) if items else ""

    def extract_all_specialties(self, filterables: List[Dict]) -> str:
        """Extract ALL specialties from filterables."""
        specialties = []
        for item in filterables:
            if item.get("slug", "").startswith("specialty_v2_"):
                specialties.append(item.get("name", ""))
        return ", ".join(specialties) if specialties else ""

    def extract_treatment_approaches_detailed(
        self, filterables: List[Dict]
    ) -> str:
        """Extract treatment approaches in detailed format."""
        modalities = []
        for item in filterables:
            if item.get("slug", "").startswith("modality_"):
                modalities.append(item.get("name", ""))
        return ", ".join(modalities) if modalities else ""

    def extract_appointment_types_detailed(
        self, filterables: List[Dict]
    ) -> str:
        """Extract appointment types in detailed format."""
        services = []
        for item in filterables:
            if item.get("slug", "").startswith("service_"):
                services.append(item.get("name", ""))
        return ", ".join(services) if services else "Video session"

    def extract_age_groups_detailed(self, filterables: List[Dict]) -> str:
        """Extract age groups in detailed format."""
        age_groups = []
        for item in filterables:
            if item.get("slug", "").startswith("ages_served_"):
                age_groups.append(item.get("name", ""))
        return ", ".join(age_groups) if age_groups else ""

    def extract_highlights(
        self, provider_data: Dict, filterables: List[Dict]
    ) -> str:
        """Extract highlights for the provider."""
        highlights = []

        # Add states
        licensure_states = provider_data.get("licensure_states", [])
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
        accepted_insurance = provider_data.get("accepted_insurance_slugs", [])
        if accepted_insurance:
            highlights.append("Accepts your insurance")

        return ", ".join(highlights)

    def parse_rate_range(self, rate_value: str) -> tuple:
        """Parse min and max session price from rate value string."""
        if not rate_value:
            return "", ""

        try:
            rate_clean = rate_value.replace("$", "").strip()
            if "-" in rate_clean:
                min_price, max_price = rate_clean.split("-")
                return min_price.strip(), max_price.strip()
            else:
                return rate_clean, rate_clean
        except Exception as e:
            logger.warning(f"⚠️  Failed to parse rate value '{rate_value}': {e}")
            return "", ""

    def process_provider_data(self, provider_data: Dict) -> Dict:
        """Process raw provider data into structured format for storage."""
        provider_id = provider_data.get("provider_id", "Unknown")
        provider_slug = provider_data.get("slug", "")

        logger.info(f"🔧 Processing provider: {provider_id}")

        # Perform NPI lookup
        npi_data = None
        try:
            logger.info(
                f"🔍 Starting NPI lookup for {provider_data.get('first_name')} {provider_data.get('last_name')}"
            )
            npi_data = self.find_npi(provider_data)
            if npi_data:
                logger.info(f"✅ NPI found: {npi_data.get('npi_number')}")
            else:
                logger.warning(
                    f"⚠️  No NPI data found for {provider_data.get('first_name')} {provider_data.get('last_name')}"
                )
        except Exception as e:
            logger.error(f"❌ NPI lookup failed: {e}")

        # Basic info
        profile_url = (
            f"https://helloalma.com/providers/{provider_slug}/"
            if provider_slug
            else ""
        )

        # Name - format as uppercase
        full_name = provider_data.get("name", "").upper()

        # Profession and bio
        profession = provider_data.get("title", "")
        bio = provider_data.get("summary", "")

        # Filterables extraction
        filterables = provider_data.get("filterables", [])

        # Enhanced field extraction
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
        licensure_states = provider_data.get("licensure_states", [])
        licenses = ", ".join(licensure_states) if licensure_states else ""

        # Rate parsing
        rate_value = provider_data.get("rate_value", "")
        min_price, max_price = self.parse_rate_range(rate_value)

        # Construct the complete data row with NPI data
        processed_data = {
            "Url": profile_url,
            "Name": full_name,
            "NPI": npi_data.get("npi_number"),
            "Profession": profession,
            "Clinic Name": "",
            "Bio": bio,
            "Additional Focus Areas": "",
            "Treatment Approaches": treatment_approaches,
            "Appointment Types": appointment_types,
            "Communities": "",
            "Age Groups": age_groups,
            "Languages": languages,
            "Highlights": highlights,
            "Gender": gender,
            "Pronouns": "",
            "Race Ethnicity": race_ethnicity,
            "Licenses": f"Licensed {profession.split(', ')[-1] if ',' in profession else profession}",
            "Locations": "Video session: Online",
            "Education": "",
            "Faiths": "",
            "Min Session Price": min_price,
            "Max Session Price": max_price,
            "Pay Out Of Pocket Status": "Yes",  # Default for Alma
            "Individual Service Rates": rate_value,
            "General Payment Options": "Insurance, Self Pay",
            "Booking Summary": "",
            "Booking Url": profile_url,
            "Listed In States": (
                ", ".join(licensure_states) if licensure_states else ""
            ),
            "States": ", ".join(licensure_states) if licensure_states else "",
            "Listed In Websites": "Hello Alma",
            "Urls": profile_url,
            "Connect Link - Facebook": "",
            "Connect Link - Instagram": "",
            "Connect Link - LinkedIn": "",
            "Connect Link - Twitter": "",
            "Connect Link - Website": "",
            "Main Specialties": all_specialties,
            "Accepted IPs": "Various",
            "NPI": npi_data.get("npi_number") if npi_data else "",
            "Sr. NO": provider_id,
            "provider_id": provider_id,
            # Store comprehensive data
            "raw_data": provider_data,
            "npi_data": npi_data,  # Store full NPI data
            "scraped_at": datetime.now(),
            "processed_at": datetime.now().isoformat(),
        }

        logger.info(
            f"✅ Successfully processed: {full_name} (ID: {provider_id})"
        )
        if npi_data:
            logger.info(f"   🆔 NPI: {npi_data.get('npi_number')}")

        return processed_data

    def scrape_and_store(self, pages: int = 1, limit: int = 15) -> List[Dict]:
        """Main method to scrape data from multiple pages and store."""
        all_processed_data = []
        total_providers_processed = 0
        successful_storages = 0
        npi_found_count = 0

        logger.info(
            f"🚀 Starting scraping process - Pages: {pages}, Limit: {limit}"
        )

        for page in range(1, pages + 1):
            logger.info(f"📄 Processing page {page}/{pages}...")

            provider_data = self.fetch_provider_list(page=page, limit=limit)
            if not provider_data:
                logger.warning(f"⚠️  Skipping page {page} due to fetch failure")
                continue

            all_extracted_providers = self.extract_all_provider_data(
                provider_data
            )
            logger.info(
                f"📊 Extracted {len(all_extracted_providers)} providers from API response"
            )

            page_processed_count = 0
            page_successful_storages = 0
            page_npi_found = 0

            for provider in all_extracted_providers:
                try:
                    processed_data = self.process_provider_data(provider)
                    storage_success = self.store_data(processed_data)

                    if storage_success:
                        page_successful_storages += 1
                        successful_storages += 1

                    if processed_data.get("NPI"):
                        page_npi_found += 1
                        npi_found_count += 1

                    all_processed_data.append(processed_data)
                    page_processed_count += 1
                    total_providers_processed += 1

                    # Add delay between NPI lookups to be respectful to the API
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"❌ Error processing provider: {e}")
                    continue

            logger.info(
                f"📊 Page {page} completed: {page_processed_count} providers processed, {page_successful_storages} stored, {page_npi_found} NPI found"
            )

            # Add delay between pages
            if page < pages:
                delay = 2
                logger.info(f"⏳ Waiting {delay} seconds before next page...")
                time.sleep(delay)

        logger.info("🎉 Scraping completed successfully!")
        logger.info(f"📈 Total pages processed: {pages}")
        logger.info(
            f"👥 Total providers processed: {total_providers_processed}"
        )
        logger.info(
            f"💾 Total records successfully stored: {successful_storages}"
        )
        logger.info(f"🆔 Total NPI records found: {npi_found_count}")

        return all_processed_data

    def export_to_excel(
        self, filename: str = "exports/alma_therapists_export.xlsx"
    ) -> pd.DataFrame:
        """Export data from storage to Excel."""
        logger.info(f"💾 Exporting data to Excel: {filename}")

        try:
            # Get data from appropriate source
            if self.use_mongodb:
                cursor = self.collection.find(
                    {},
                    {
                        "raw_data": 0,
                        "scraped_at": 0,
                        "_id": 0,
                        "processed_at": 0,
                        "npi_data": 0,  # Exclude full NPI data from Excel
                    },
                )
                data_list = list(cursor)
            else:
                data_list = []
                for item in self.local_data:
                    cleaned_item = {
                        k: v
                        for k, v in item.items()
                        if k
                        not in [
                            "raw_data",
                            "scraped_at",
                            "processed_at",
                            "npi_data",
                        ]
                    }
                    data_list.append(cleaned_item)

            if not data_list:
                logger.warning("⚠️  No data found to export")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data_list)

            # Expected columns in order (including NPI)
            expected_columns = [
                "Url",
                "Name",
                "Profession",
                "NPI",
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
            ]

            # Add missing columns with empty values
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = ""

            # Reorder columns
            df = df[expected_columns]

            # Ensure exports directory exists
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            # Export to Excel
            df.to_excel(filename, index=False, engine="openpyxl")
            logger.info(f"✅ Excel file created successfully: {filename}")
            logger.info(f"📊 Export Summary: {len(df)} therapists")
            logger.info(f"🆔 NPI Records in Export: {df['NPI'].notna().sum()}")

            return df

        except Exception as e:
            logger.error(f"❌ Error during Excel export: {e}")
            raise

    def export_comprehensive_data(
        self, filename: str = "exports/alma_therapists_comprehensive.json"
    ):
        """
        Export comprehensive data including full NPI information to JSON.
        """
        try:
            if self.use_mongodb:
                cursor = self.collection.find({})
                data_list = list(cursor)
            else:
                data_list = self.local_data

            export_data = {
                "export_metadata": {
                    "export_date": datetime.now().isoformat(),
                    "total_providers": len(data_list),
                    "npi_records_count": sum(
                        1 for item in data_list if item.get("npi_data")
                    ),
                    "version": "2.0",
                },
                "providers": data_list,
            }

            # Ensure exports directory exists
            os.makedirs(os.path.dirname(filename), exist_ok=True)

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(
                    export_data, f, indent=2, ensure_ascii=False, default=str
                )

            logger.info(f"💾 Comprehensive data exported to: {filename}")
            logger.info(
                f"📊 Includes full NPI data for {export_data['export_metadata']['npi_records_count']} providers"
            )

        except Exception as e:
            logger.error(f"❌ Error exporting comprehensive data: {e}")

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
    """Main execution function for the Alma Therapist Scraper."""
    logger.info("🎬 Starting Alma Therapist Data Scraper")

    # Get configuration from environment
    pages = int(os.getenv("SCRAPE_PAGES", "17000"))
    limit = int(os.getenv("SCRAPE_PAGE_LIMIT", "15"))

    # Initialize the scraper
    scraper = AlmaTherapistScraper(use_mongodb=True)

    try:
        # Display startup information
        logger.info("🚀 Configuration:")
        logger.info(f"   • Target: Hello Alma Therapist Directory")
        logger.info(f"   • Pages: {pages}")
        logger.info(f"   • Limit per page: {limit}")

        # Scrape data
        logger.info("🌐 Beginning data scraping process...")
        scraper.scrape_and_store(pages=pages, limit=limit)

        # Export to Excel
        logger.info("💾 Beginning Excel export process...")
        df = scraper.export_to_excel("exports/alma_therapists_export.xlsx")

        # Export comprehensive data with NPI information
        logger.info("💾 Exporting comprehensive data with NPI...")
        scraper.export_comprehensive_data(
            "exports/alma_therapists_comprehensive.json"
        )

        if not df.empty:
            logger.info("🎉 Process completed successfully!")
            logger.info(f"📁 Output files:")
            logger.info(f"   • Excel: exports/alma_therapists_export.xlsx")
            logger.info(
                f"   • JSON: exports/alma_therapists_comprehensive.json"
            )
            logger.info(f"📊 Final count: {len(df)} therapists exported")
            logger.info(f"🆔 NPI records found: {df['NPI'].notna().sum()}")
        else:
            logger.warning("⚠️  Process completed but no data was exported")

    except Exception as e:
        logger.error(f"💥 Fatal error in main process: {e}")

    finally:
        scraper.close()
        logger.info("🏁 Alma Therapist Scraper has finished execution")


if __name__ == "__main__":
    main()
