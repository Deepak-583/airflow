"""
Data processing utility functions.
"""
import json
import ast
import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


def clean_event_data(raw_data: List[Any]) -> List[Dict[str, Any]]:
    """
    Clean and normalize event data from raw JSON.
    
    Args:
        raw_data: List of raw event data (can be strings or dicts)
        
    Returns:
        List of cleaned event dictionaries
        
    Raises:
        ValueError: If no valid records found after cleaning
    """
    records = []
    
    for item in raw_data:
        # Convert JSON string to dict if needed
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                logger.warning("Skipping invalid JSON string entry")
                continue
        
        if not isinstance(item, dict):
            logger.warning(f"Skipping non-dict item: {type(item)}")
            continue
        
        # Convert props string → dict
        if isinstance(item.get("props"), str):
            try:
                item["props"] = ast.literal_eval(item["props"])
            except (ValueError, SyntaxError):
                logger.warning("Failed to parse props, setting to empty dict")
                item["props"] = {}
        
        # Convert geo_location string → dict
        if isinstance(item.get("geo_location"), str):
            try:
                item["geo_location"] = ast.literal_eval(item["geo_location"])
            except (ValueError, SyntaxError):
                logger.warning("Failed to parse geo_location, setting to empty dict")
                item["geo_location"] = {}
        
        records.append(item)
    
    if not records:
        raise ValueError("No valid cleaned records found after processing")
    
    logger.info(f"Cleaned {len(records)} records from {len(raw_data)} raw items")
    return records


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame column names to lowercase.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with normalized column names
    """
    df.columns = [col.lower() for col in df.columns]
    logger.debug(f"Normalized {len(df.columns)} column names to lowercase")
    return df

