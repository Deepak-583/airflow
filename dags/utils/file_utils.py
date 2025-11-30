"""
File utility functions for file operations.
"""
import os
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def ensure_directory_exists(directory: Path) -> None:
    """Ensure a directory exists, create if it doesn't."""
    directory.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Directory ensured: {directory}")


def get_latest_file(directory: Path, extension: str) -> Optional[str]:
    """
    Get the latest file in a directory with the specified extension.
    
    Args:
        directory: Path to the directory
        extension: File extension (e.g., '.json', '.csv')
        
    Returns:
        Path to the latest file or None if no files found
    """
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return None
    
    files = [
        directory / f
        for f in os.listdir(directory)
        if f.endswith(extension)
    ]
    
    if not files:
        logger.warning(f"No {extension} files found in {directory}")
        return None
    
    # Sort by modification time, most recent first
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    latest_file = str(files[0])
    logger.info(f"Latest {extension} file: {latest_file}")
    return latest_file


def read_json_file(filepath: str) -> list:
    """
    Read and parse a JSON file.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        Parsed JSON data as a list
        
    Raises:
        FileNotFoundError: If file doesn't exist or path is None
        json.JSONDecodeError: If file is not valid JSON
    """
    if not filepath or filepath == "None":
        raise FileNotFoundError(f"Invalid filepath: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    import json
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    logger.info(f"Read JSON file: {filepath} ({len(data) if isinstance(data, list) else 'N/A'} items)")
    return data


def write_json_file(filepath: str, data: list, indent: int = 2) -> None:
    """
    Write data to a JSON file.
    
    Args:
        filepath: Path to write the JSON file
        data: Data to write (must be JSON serializable)
        indent: JSON indentation level
    """
    import json
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
    
    logger.info(f"Wrote JSON file: {filepath}")

