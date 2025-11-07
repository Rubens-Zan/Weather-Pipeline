# Add your imports here
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
import requests.sessions  # For type hinting

# Add any utility functions here if needed
# --- Constants ---
TASKS_FILE = Path("tasks.json")
API_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

# Location coordinates as specified in README.md
LOCATION_COORDINATES: Dict[str, Dict[str, float]] = {
    "amsterdam": {"latitude": 52.37, "longitude": 4.89},
    "london": {"latitude": 51.51, "longitude": -0.13},
    "paris": {"latitude": 48.8566, "longitude": 2.3522 }
}

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def fetch_weather_data(
    session: requests.sessions.Session, 
    latitude: float, 
    longitude: float, 
    date: str, 
    sensors: List[str]
) -> Dict[str, Any]:
    """
    Fetches data for a single day from the Open-Meteo API.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date,
        "end_date": date,
        "hourly": ",".join(sensors),
        "timezone": "UTC"  # Use UTC for consistency
    }
    response = session.get(API_ENDPOINT, params=params)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
    return response.json()

def convert_to_long_format(
    api_response: Dict[str, Any], 
    location_name: str
) -> Optional[pd.DataFrame]:
    """
    Converts the JSON response from Open-Meteo API to a LONG format DataFrame.
    Returns None if no data is present in the response.
    """
    hourly_data = api_response.get("hourly", {})
    if "time" not in hourly_data or not hourly_data["time"]:
        # No data returned for this period
        return None

    df = pd.DataFrame(hourly_data)
    
    # 1. Convert 'time' to 'timestamp' in UTC
    df.rename(columns={"time": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    
    # 2. Melt to LONG format: (timestamp, sensor_name, value)
    df_long = df.melt(
        id_vars=["timestamp"],
        var_name="sensor_name",
        value_name="value"
    )
    
    # 3. Add location column
    df_long["location"] = location_name
    
    # 4. Reorder columns for clarity
    df_long = df_long[["timestamp", "location", "sensor_name", "value"]]
    
    return df_long


def scrape():
    # Implement the API scrape logic here
    log.info("--- Starting Scrape Stage ---")
    # 1. Load tasks.json to get the list of dates and locations to scrape
    if not TASKS_FILE.exists():
        log.error(f"Tasks file not found: {TASKS_FILE}. Run 'parametrize' stage first.")
        raise FileNotFoundError(f"{TASKS_FILE} not found")

    try:
        with open(TASKS_FILE, 'r') as f:
            task_data = json.load(f)
        tasks: List[Dict[str, Any]] = task_data.get("tasks", [])
        log.info(f"Loaded {len(tasks)} tasks from {TASKS_FILE}")
    except Exception as e:
        log.error(f"Failed to load or parse {TASKS_FILE}: {e}")
        raise
    
    with requests.Session() as session:
        for task in tasks:
            try:
                location_name: str = task["location_name"]
                # Convert ISO string from tasks.json back to "YYYY-MM-DD" format for API
                date_str: str = pd.to_datetime(task["date"]).strftime("%Y-%m-%d")
                sensors: List[str] = task["sensors"]
                raw_file_path = Path(task["raw_file_path"])

                log.info(f"Processing task: {location_name} on {date_str}")

                # 2. Fetch data from Open-Meteo Archive API for each task
                coords = LOCATION_COORDINATES.get(location_name)
                if not coords:
                    log.warning(f"No coordinates for {location_name}, skipping task.")
                    continue

                api_response = fetch_weather_data(
                    session, coords["latitude"], coords["longitude"], date_str, sensors
                )

                # 3. Convert API response to LONG format (timestamp, location, sensor_name, value)
                df_long = convert_to_long_format(api_response, location_name)
                
                if df_long is None or df_long.empty:
                    log.warning(f"No data returned for {location_name} on {date_str}, skipping file write.")
                    continue

                # 4. Write daily parquet files to raw_output_dir
                # Ensure parent directory exists
                raw_file_path.parent.mkdir(parents=True, exist_ok=True)
                df_long.to_parquet(raw_file_path, index=False, engine="pyarrow")
                
                log.info(f"Successfully wrote {len(df_long)} rows to {raw_file_path}")

            except requests.exceptions.RequestException as e:
                # Handle errors gracefully (log and continue) as per README
                log.error(f"API Error processing {task.get('location_name')} for {task.get('date')}: {e}")
            except Exception as e:
                log.error(f"Failed processing task {task}: {e}")
                # Continue processing other tasks
            
            log.info("--- Scrape Stage Complete ---")

    # raise NotImplementedError
