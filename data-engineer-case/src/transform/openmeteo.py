# Add your imports here
import json
import logging
from pathlib import Path
from collections import defaultdict
from typing import List, Optional, Dict, Any

import pandas as pd

# --- Constants ---
TASKS_FILE = Path("tasks.json")

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Add any utility functions here if needed
def load_raw_data(raw_file_paths: List[str]) -> Optional[pd.DataFrame]:
    """
    Loads multiple raw parquet files into a single DataFrame.
    Returns None if no files are found or loaded.
    """
    all_raw_dfs = []
    for file_path in raw_file_paths:
        p = Path(file_path)
        if not p.exists():
            log.warning(f"Raw file not found: {file_path}. Skipping.")
            continue
        try:
            df = pd.read_parquet(file_path)
            all_raw_dfs.append(df)
        except Exception as e:
            log.warning(f"Could not read raw file {file_path}: {e}. Skipping.")
    
    if not all_raw_dfs:
        return None
        
    return pd.concat(all_raw_dfs, ignore_index=True)

def convert_to_wide_format(df_long: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Converts the LONG format DataFrame to WIDE format.
    Returns None if input is None or empty.
    """
    if df_long is None or df_long.empty:
        return None
        
    # 3. Convert LONG to WIDE (pivot sensor_name into columns)
    df_wide = df_long.pivot(
        index=["timestamp", "location"],
        columns="sensor_name",
        values="value"
    ).reset_index()
    
    # Clean up column names (if pandas adds a name to the column index)
    df_wide.columns.name = None
    
    # Ensure timestamp is datetime and in UTC
    df_wide["timestamp"] = pd.to_datetime(df_wide["timestamp"], utc=True)
    
    return df_wide

def merge_data(df_new: pd.DataFrame, df_historical: pd.DataFrame) -> pd.DataFrame:
    """
    Merges new WIDE data with historical WIDE data, handling duplicates.
    """
    # 5. Merge new data with historical data
    # Use outer join to keep all columns from both (e.g., 'dew_point' from hist)
    df_combined = pd.concat([df_historical, df_new], ignore_index=True, join="outer")
    
    # Sort by timestamp, which is crucial for de-duplication
    df_combined.sort_values(by="timestamp", inplace=True)
    
    # Handle duplicates: Keep the 'last' entry (new data) for any (timestamp, location) pair
    df_combined.drop_duplicates(subset=["timestamp", "location"], keep="last", inplace=True)
    
    # Ensure final timestamp is in UTC
    # Per README: "Timestamps in millisecond precision UTC timezone"
    # pandas to_datetime with utc=True handles this.
    df_combined["timestamp"] = pd.to_datetime(df_combined["timestamp"], utc=True)

    return df_combined

def transform():
    # Implement the transform logic here
    # 1. Load tasks.json to get the list of dates and locations to process
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
    
    # 2. Load all raw LONG format parquet files for the date range
    tasks_by_month: Dict[str, List[str]] = defaultdict(list)
    for task in tasks:
        # structured_file_path is e.g., "data/structured/amsterdam/202410.parquet"
        key = task["structured_file_path"]
        tasks_by_month[key].append(task["raw_file_path"])

    log.info(f"Grouped tasks into {len(tasks_by_month)} monthly files to update.")

    # 3. Convert LONG format to WIDE format (pivot sensor_name into columns)
    for structured_file, raw_files in tasks_by_month.items():
        structured_path = Path(structured_file)
        log.info(f"Processing monthly file: {structured_path}")
        
        try:
            # 3a. Load all raw LONG format files for this group
            df_long = load_raw_data(raw_files)
            if df_long is None or df_long.empty:
                log.warning(f"No raw data found for {structured_file}. Skipping.")
                continue
            log.info(f"Loaded {len(df_long)} total rows from {len(raw_files)} raw files.")

            # 3b. Convert LONG to WIDE
            df_new_wide = convert_to_wide_format(df_long)
            if df_new_wide is None or df_new_wide.empty:
                log.warning(f"Pivoting raw data for {structured_file} resulted in empty DataFrame. Skipping.")
                continue
            log.info(f"Pivoted raw data to {df_new_wide.shape[0]} rows (WIDE format).")

            # 4. Load existing historical data from structured_output_dir
            df_historical: Optional[pd.DataFrame] = None
            if structured_path.exists():
                try:
                    df_historical = pd.read_parquet(structured_path)
                    # Ensure timestamp is correct type for merging
                    df_historical["timestamp"] = pd.to_datetime(df_historical["timestamp"], utc=True)
                    log.info(f"Loaded {len(df_historical)} rows from existing historical file {structured_path}")
                except Exception as e:
                    log.error(f"Failed to read historical file {structured_path}: {e}. Will overwrite.")
            else:
                log.info(f"No historical file found at {structured_path}. A new file will be created.")

            # 5. Merge new data with historical data (handle duplicates and schema differences)
            df_final: pd.DataFrame
            if df_historical is not None:
                df_final = merge_data(df_new_wide, df_historical)
            else:
                # No historical data, just use the new data (but still sort/dedupe)
                df_final = df_new_wide.sort_values(by="timestamp")
                df_final.drop_duplicates(subset=["timestamp", "location"], keep="last", inplace=True)
            
            log.info(f"Merged data. Final row count for {structured_path}: {len(df_final)}")

            # 6. Write monthly parquet files to structured_output_dir
            structured_path.parent.mkdir(parents=True, exist_ok=True)
            df_final.to_parquet(structured_path, index=False, engine="pyarrow")
            
            log.info(f"Successfully wrote {len(df_final)} rows to {structured_path}")

        except Exception as e:
            log.error(f"Failed to process monthly file {structured_path}: {e}")
            # Continue to the next monthly file
        log.info("--- Transform Stage Complete ---")
    
    
    
    # raise NotImplementedError
