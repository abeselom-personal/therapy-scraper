import asyncio
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
import requests
from pymongo import MongoClient, UpdateOne

# Environment variables
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "therapyfinder_speed_test")
MONGO_USER = os.getenv("MONGO_USER", "scraper")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "scraper")
API_URL = os.getenv("API_URL", "https://therapyfinder.com/api/clinicians")
SCRAPE_LIMIT = int(os.getenv("SCRAPE_LIMIT", "0"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))


class RateLimiter:
    """Rate limiter to control request frequency"""

    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            # Remove calls outside the current period
            self.calls = [
                call for call in self.calls if now - call < self.period
            ]

            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                self.calls = self.calls[1:]

            self.calls.append(time.time())


# Global rate limiter (10 requests per second)
rate_limiter = RateLimiter(max_calls=10, period=1.0)


class MongoDBManager:
    """Thread-safe MongoDB connection manager"""

    def __init__(self):
        self._client = None
        self._db = None
        self._lock = asyncio.Lock()

    async def get_client(self):
        async with self._lock:
            if self._client is None:
                self._client = await self._create_client()
                self._db = self._client[MONGO_DB]
            return self._client, self._db

    async def _create_client(self):
        connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"

        client = MongoClient(
            connection_string,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            socketTimeoutMS=30000,
            maxPoolSize=MAX_CONCURRENT_REQUESTS * 2,
        )

        # Test connection
        client.admin.command("ping")
        return client

    async def close(self):
        if self._client:
            self._client.close()


# Global MongoDB manager
mongo_manager = MongoDBManager()


async def wait_for_mongodb(max_retries=30, retry_interval=5):
    """Wait for MongoDB to be ready"""
    print("⏳ Waiting for MongoDB to be ready...")

    for i in range(max_retries):
        print(f"Attempt {i+1}/{max_retries}...")
        try:
            client, db = await mongo_manager.get_client()
            # Test database access
            collections = db.list_collection_names()
            print(f"✅ Database access verified. Collections: {collections}")
            return True
        except Exception as e:
            print(f"❌ MongoDB not ready yet: {e}")
            if i < max_retries - 1:
                print(f"Waiting {retry_interval} seconds before retry...")
                await asyncio.sleep(retry_interval)

    print(f"❌ Failed to connect to MongoDB after {max_retries} attempts")
    return False


async def ensure_indexes():
    """Create necessary indexes"""
    try:
        print("Creating database indexes...")
        client, db = await mongo_manager.get_client()

        # Create collections if they don't exist
        if "clinicians" not in db.list_collection_names():
            db.create_collection("clinicians")
        if "raw_pages" not in db.list_collection_names():
            db.create_collection("raw_pages")

        # Create indexes concurrently
        index_operations = [
            db.clinicians.create_index("clinician_id", unique=True),
            db.clinicians.create_index("location_state"),
            db.clinicians.create_index("location_city"),
            db.clinicians.create_index("telehealth"),
            db.clinicians.create_index("accepts_insurance"),
            db.raw_pages.create_index([("state", 1), ("city", 1), ("page", 1)]),
            db.raw_pages.create_index("scrape_timestamp"),
        ]

        print("✅ Database indexes created/verified")
        return True
    except Exception as e:
        print(f"❌ Error creating indexes: {e}")
        return False


def get_all_states():
    """Fetches and returns a de-duplicated list of all states from the API."""
    url = "https://therapyfinder.com/api/browse-states"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        all_states = []
        for item in data["data"]:
            states_list = item["attributes"].get("states", [])
            all_states.extend(states_list)

        # Remove duplicates while preserving order
        unique_states = list(dict.fromkeys(all_states))
        print(f"✅ Found {len(unique_states)} unique states.")
        return unique_states

    except requests.RequestException as e:
        print(f"❌ Failed to fetch states: {e}")
        return []


def get_cities_for_state(state_name):
    """Fetches and returns a list of cities for a given state."""
    state_param = state_name.lower()
    url = f"https://therapyfinder.com/api/browse-cities?filter[state]={state_param}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        all_cities = []
        for item in data["data"]:
            cities_list = item["attributes"].get("cities", [])
            all_cities.extend(cities_list)

        unique_cities = list(dict.fromkeys(all_cities))
        print(f"✅ Found {len(unique_cities)} cities in {state_name}.")
        return unique_cities

    except requests.RequestException as e:
        print(f"❌ Failed to fetch cities for {state_name}: {e}")
        return []


async def store_raw_pages_batch(raw_pages_batch: List[Dict]):
    """Store multiple raw pages in a single batch operation"""
    if not raw_pages_batch:
        return

    try:
        client, db = await mongo_manager.get_client()
        result = db.raw_pages.insert_many(raw_pages_batch, ordered=False)
        return len(result.inserted_ids)
    except Exception as e:
        print(f"❌ Error storing raw pages batch: {e}")
        return 0


async def process_clinicians_batch(clinicians_batch: List[Dict]):
    """Process and store multiple clinicians in a single batch operation"""
    if not clinicians_batch:
        return

    try:
        client, db = await mongo_manager.get_client()

        operations = []
        for clinician_data in clinicians_batch:
            operation = UpdateOne(
                {"clinician_id": clinician_data["clinician_id"]},
                {
                    "$set": clinician_data,
                    "$setOnInsert": {"created_at": datetime.utcnow()},
                },
                upsert=True,
            )
            operations.append(operation)

        if operations:
            result = db.clinicians.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
    except Exception as e:
        print(f"❌ Error processing clinicians batch: {e}")

    return 0


async def fetch_page(
    session: aiohttp.ClientSession, url: str, params: Dict
) -> Optional[Dict]:
    """Fetch a single page with rate limiting and error handling"""
    await rate_limiter.acquire()

    try:
        async with session.get(url, params=params, timeout=30) as response:
            response.raise_for_status()
            return await response.json()
    except asyncio.TimeoutError:
        print(f"❌ Timeout fetching page with params: {params}")
    except aiohttp.ClientError as e:
        print(f"❌ Error fetching page: {e}")
    except Exception as e:
        print(f"❌ Unexpected error fetching page: {e}")

    return None


async def scrape_city_concurrent(
    city_param: str, state: str, city_name: str, semaphore: asyncio.Semaphore
):
    """Scrape all pages for a specific city concurrently"""
    async with semaphore:
        print(f"🎯 Starting to scrape city: {city_name} in state: {state}")

        total_city_records = 0
        page = 1
        has_more_pages = True

        raw_pages_batch = []
        clinicians_batch = []

        async with aiohttp.ClientSession() as session:
            while has_more_pages:
                if SCRAPE_LIMIT > 0 and total_city_records >= SCRAPE_LIMIT:
                    print(f"⏹️ Reached scrape limit for city {city_name}")
                    break

                # API parameters
                params = {
                    "featureFlags[featureUseActiveLocationList]": "true",
                    "featureFlags[featureUseStatewideTelehealth]": "true",
                    "filter[directoryPageUrl]": city_param,
                    "include": "specialties,insuranceCarriers,clinicianProfessionalLicenses.globalLicenseType,offices,availabilities,practice",
                    "page[size]": 20,
                    "page[number]": page,
                }

                data = await fetch_page(session, API_URL, params)
                if data and "included" in data:
                    linked = link_relationships(data["data"], data["included"])
                    for k, v in linked.items():
                        clinician = next(
                            (c for c in data["data"] if c["id"] == k), None
                        )
                        if clinician:
                            clinician["linked"] = v["relationships"]
                if not data or not data.get("data"):
                    print(f"❌ No data found for {city_name} page {page}")
                    break

                # Store raw page in batch
                raw_page = {
                    "state": state,
                    "city": city_name,
                    "page": page,
                    "scrape_timestamp": datetime.utcnow(),
                    "data": data,
                }
                raw_pages_batch.append(raw_page)

                # Process clinicians
                for clinician in data["data"]:
                    if SCRAPE_LIMIT > 0 and total_city_records >= SCRAPE_LIMIT:
                        break

                    clinician_data = process_single_clinician(
                        clinician, state, city_name
                    )
                    if clinician_data:
                        clinicians_batch.append(clinician_data)
                        total_city_records += 1

                # Process batches if they reach the batch size
                if len(raw_pages_batch) >= BATCH_SIZE:
                    await store_raw_pages_batch(raw_pages_batch)
                    raw_pages_batch = []

                if len(clinicians_batch) >= BATCH_SIZE:
                    await process_clinicians_batch(clinicians_batch)
                    clinicians_batch = []

                print(
                    f"✅ Page {page} for {city_name} processed. Records: {len(data['data'])}, Total for city: {total_city_records}"
                )

                # Check for next page
                if "next" not in data.get("links", {}):
                    has_more_pages = False
                else:
                    page += 1

        # Process any remaining batches
        if raw_pages_batch:
            await store_raw_pages_batch(raw_pages_batch)
        if clinicians_batch:
            await process_clinicians_batch(clinicians_batch)

        print(
            f"✅ Completed scraping {city_name}. Total records: {total_city_records}"
        )
        return total_city_records


def process_single_clinician(
    clinician: Dict, state: str, city_name: str
) -> Optional[Dict]:
    """Process a single clinician and return data for batch insertion"""
    try:
        attributes = clinician["attributes"]
        clinician_data = {
            "clinician_id": clinician["id"],
            "type": clinician["type"],
            "links": clinician.get("links", {}),
            "attributes": attributes,
            "location_state": state,
            "location_city": city_name,
            "scraped_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "first_name": attributes.get("firstName"),
            "last_name": attributes.get("lastName"),
            "display_name": attributes.get("displayName"),
            "telehealth": attributes.get("telehealthIndicator", False),
            "accepts_insurance": attributes.get(
                "acceptInsuranceIndicator", False
            ),
            "accept_new_clients": attributes.get(
                "acceptNewClientsIndicator", True
            ),
            "profile_img": attributes.get("profileImgUrl"),
            "title": attributes.get("title"),
            "bio": attributes.get("bio"),
        }
        return clinician_data
    except Exception as e:
        print(f"❌ Error processing clinician data: {e}")
        return None


async def scrape_state_concurrent(state: str, semaphore: asyncio.Semaphore):
    """Scrape all cities in a state concurrently"""
    print(f"\n🏁 Processing state: {state}")
    cities = get_cities_for_state(state)

    if not cities:
        print(f"⚠️ No cities found for {state}, skipping...")
        return 0

    # Create tasks for all cities in this state
    tasks = []
    for city in cities:
        if (
            SCRAPE_LIMIT > 0
            and sum(task.result() for task in tasks if task.done())
            >= SCRAPE_LIMIT
        ):
            break

        city_name, state_code = map(str.strip, city.split(","))
        city_param = (
            f"{city_name.lower().replace(' ', '-')}-{state_code.lower()}"
        )

        task = asyncio.create_task(
            scrape_city_concurrent(city_param, state, city_name, semaphore)
        )
        tasks.append(task)

    # Wait for all city tasks to complete with timeout
    try:
        state_results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=3600,  # 1 hour timeout per state
        )
    except asyncio.TimeoutError:
        print(f"❌ Timeout processing state {state}")
        return 0

    # Process results
    state_records = 0
    for result in state_results:
        if isinstance(result, Exception):
            print(f"❌ Error in city task: {result}")
        else:
            state_records += result

    print(f"✅ Completed state: {state}. Total records: {state_records}")
    return state_records


async def main():
    print("🚀 Starting Optimized TherapyFinder Scraper...")

    # Wait for MongoDB to be ready
    if not await wait_for_mongodb():
        print("❌ Exiting due to MongoDB connection failure")
        sys.exit(1)

    try:
        # Ensure indexes exist
        if not await ensure_indexes():
            print("⚠️ Failed to create indexes, but continuing...")

        # Get all states dynamically
        STATES = get_all_states()
        if not STATES:
            print("❌ Exiting: Could not retrieve state list.")
            return

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        total_records = 0
        total_states_processed = 0

        # Process states with limited concurrency
        for i in range(0, len(STATES), MAX_CONCURRENT_REQUESTS // 2):
            if SCRAPE_LIMIT > 0 and total_records >= SCRAPE_LIMIT:
                print(
                    f"⏹️ Reached global scrape limit of {SCRAPE_LIMIT} records"
                )
                break

            batch_states = STATES[i : i + MAX_CONCURRENT_REQUESTS // 2]
            print(f"\n📦 Processing batch of {len(batch_states)} states...")

            # Create tasks for current batch of states
            state_tasks = [
                scrape_state_concurrent(state, semaphore)
                for state in batch_states
            ]

            # Wait for batch to complete
            batch_results = await asyncio.gather(
                *state_tasks, return_exceptions=True
            )

            # Process batch results
            for state, result in zip(batch_states, batch_results):
                if isinstance(result, Exception):
                    print(f"❌ Error processing state {state}: {result}")
                else:
                    total_records += result
                    total_states_processed += 1

        print(f"\n🎉 SCRAPING COMPLETED")
        print(f"📊 Total states processed: {total_states_processed}")
        print(f"📊 Total records processed: {total_records}")

        # Print statistics
        await print_statistics()

    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await mongo_manager.close()
        print("🔌 MongoDB connection closed")


def link_relationships(data, included):
    included_map = {(i["type"], i["id"]): i for i in included}
    linked = {}
    for item in data:
        relationships = item.get("relationships", {})
        linked_item = {
            "id": item["id"],
            "type": item["type"],
            "relationships": {},
        }
        for rel_name, rel_data in relationships.items():
            rel_objects = rel_data.get("data")
            if isinstance(rel_objects, list):
                linked_item["relationships"][rel_name] = [
                    included_map.get((r["type"], r["id"]))
                    for r in rel_objects
                    if included_map.get((r["type"], r["id"])) is not None
                ]
            elif isinstance(rel_objects, dict):
                linked_item["relationships"][rel_name] = included_map.get(
                    (rel_objects["type"], rel_objects["id"])
                )
        linked[item["id"]] = linked_item
    return linked


async def print_statistics():
    """Print scraping statistics"""
    try:
        client, db = await mongo_manager.get_client()

        clinicians_count = db.clinicians.count_documents({})
        raw_pages_count = db.raw_pages.count_documents({})

        print(f"👥 Total clinicians in database: {clinicians_count}")
        print(f"📄 Total raw pages stored: {raw_pages_count}")

        # Print state-wise counts
        print("\n📈 Records per state:")
        pipeline = [
            {"$group": {"_id": "$location_state", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        state_counts = list(db.clinicians.aggregate(pipeline))
        for state_count in state_counts:
            print(f"  {state_count['_id']}: {state_count['count']}")

        # Print city-wise counts for top cities
        print("\n🏙️ Records per city (top 10):")
        pipeline = [
            {"$group": {"_id": "$location_city", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ]
        city_counts = list(db.clinicians.aggregate(pipeline))
        for city_count in city_counts:
            print(f"  {city_count['_id']}: {city_count['count']}")

    except Exception as e:
        print(f"❌ Error getting statistics: {e}")


if __name__ == "__main__":
    # Set higher memory limits for better performance
    if sys.platform != "win32":
        import resource

        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

    asyncio.run(main())
