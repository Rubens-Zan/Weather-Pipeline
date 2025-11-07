# Add your imports here
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ValidationError, field_validator
import pandas as pd
import requests
import requests.sessions # For type hinting
import re  # <-- Import regular expressions
from datetime import timedelta

# Add any utility functions here if needed
class DateConfig(BaseModel):
    begin_date: str
    end_date: str
    time_increment: str

#
class Location(BaseModel):
    name: str
    sensors: List[str]

    @field_validator('name')
    @classmethod
    def validate_name_is_empty(cls, v: str) -> str: 
        """_summary_

        Args:
            v (str): Location string received by API 

        Raises:
            ValueError: 
            FileNotFoundError: _description_

        Returns:
            str: _description_
        """
        if not v.strip(): 
            raise ValueError('ERRO: Nome da localização é vazio ')
        return v 

class LocalStorage(BaseModel):
    raw_output_dir: str
    structured_output_dir: str

class Workload(BaseModel):
    date_config: DateConfig
    locations: List[Location]
    local_storage: LocalStorage

# --- Constants ---
WORKLOAD_FILE = Path("workload.json")
TASKS_FILE = Path("tasks.json")

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)



def parse_iso8601_duration(duration_str: str) -> timedelta:
    """
    Parses a simplified ISO 8601 duration string (e.g., +P1DT00H00M00S). 
    """
    # Regex to capture the days, hours, minutes, and seconds
    # Format: +P[days]DT[hours]H[minutes]M[seconds]S
    match = re.match(
        r'\+P(\d+)DT(\d+)H(\d+)M(\d+)S', 
        duration_str
    )
    
    if not match:
        log.error(f"Invalid time_increment format: {duration_str}")
        raise ValueError(f"Invalid time_increment format: {duration_str}")

    try:
        # Extract groups and convert to integers
        days, hours, minutes, seconds = map(int, match.groups())
        
        # Create the timedelta object
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    except Exception as e:
        log.error(f"Error parsing time_increment '{duration_str}': {e}")
        raise

def parametrize():
    # Implement the parametrize logic here
    # 1. Load and validate workload.json configuration file
    log.info("--- Starting Parametrize Stage ---")
    if not WORKLOAD_FILE.exists():
        log.error(f"Configuration file not found: {WORKLOAD_FILE}")
        raise FileNotFoundError(f"{WORKLOAD_FILE} not found")
    
    try:
        with open(WORKLOAD_FILE, 'r') as f:
            config_data = json.load(f)
        workload = Workload.model_validate(config_data)
        log.info(f"Successfully loaded and validated {WORKLOAD_FILE}")
    except Exception as e:
        log.error(f"Failed to load or validate {WORKLOAD_FILE}: {e}")
        raise
    # 2. Parse ISO 8601 duration format from time_increment field (e.g., +P1DT00H00M00S)
    try:
        # We use our new helper function instead of pd.to_timedelta
        time_delta = parse_iso8601_duration(workload.date_config.time_increment)
        log.info(f"Parsed time_increment '{workload.date_config.time_increment}' to {time_delta}")
    except ValueError as e:
        # This will now catch errors from our parse_iso8601_duration function
        log.error(f"Invalid time_increment format: {e}")
        raise

    # 3. Generate list of dates between begin_date and end_date using time_increment
    dates = pd.date_range(
        start=workload.date_config.begin_date,
        end=workload.date_config.end_date,
        freq=time_delta,
        tz="UTC" # Use UTC for consistency
    )
    log.info(f"Generated {len(dates)} dates from {workload.date_config.begin_date} to {workload.date_config.end_date}")

    # 4. Create tasks for each location and date combination
    tasks = []
    raw_path_template = workload.local_storage.raw_output_dir # e.g., "data/raw/{location_name}/%Y%m%d.parquet"
    structured_path_template = workload.local_storage.structured_output_dir # e.g., "data/structured/{location_name}/%Y%m.parquet"

    for location in workload.locations:
        location_name = location.name
        log.info(f"Generating tasks for location: {location_name}")
        
        # Resolve the {location_name} placeholder
        location_raw_template = raw_path_template.format(location_name=location_name)
        location_structured_template = structured_path_template.format(location_name=location_name)
        
        for date in dates:
            # Resolve the date formatting (e.g., %Y%m%d)
            raw_file_path = date.strftime(location_raw_template)
            structured_file_path = date.strftime(location_structured_template)

            task = {
                "location_name": location_name,
                "sensors": location.sensors,
                "date": date.isoformat(), # Store date in standard ISO format
                "raw_file_path": raw_file_path,
                "structured_file_path": structured_file_path # Pass this along to make transform stage easier
            }
            tasks.append(task)

    # 5. Write tasks to tasks.json file for use in scrape and transform stages
    output_data = {
        "workload_config": workload.model_dump(),
        "tasks": tasks
    }

    try:
        with open(TASKS_FILE, 'w') as f:
            json.dump(output_data, f, indent=2)
        log.info(f"Successfully generated {len(tasks)} tasks and saved to {TASKS_FILE}")
    except IOError as e:
        log.error(f"Failed to write tasks to {TASKS_FILE}: {e}")
        raise

    log.info("--- Parametrize Stage Complete ---")

    # raise NotImplementedError
