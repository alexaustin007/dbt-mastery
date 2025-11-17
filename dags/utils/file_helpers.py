"""
Helper functions for file management in the pipeline
"""
import os
import glob
import shutil
from datetime import datetime


def find_latest_csv(directory, pattern='*.csv'):
    """
    Find the most recent CSV file in a directory

    Args:
        directory: Path to search
        pattern: File pattern to match

    Returns:
        Path to the latest CSV file or None
    """
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None

    # Sort by modification time, return latest
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def move_file(source, destination_dir):
    """
    Move file to destination directory with timestamp

    Args:
        source: Source file path
        destination_dir: Destination directory

    Returns:
        New file path
    """
    if not os.path.exists(source):
        raise FileNotFoundError(f"Source file not found: {source}")

    # Create destination directory if needed
    os.makedirs(destination_dir, exist_ok=True)

    # Add timestamp to filename
    filename = os.path.basename(source)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    name, ext = os.path.splitext(filename)
    new_filename = f"{name}_{timestamp}{ext}"

    destination = os.path.join(destination_dir, new_filename)
    shutil.move(source, destination)

    return destination


def check_file_exists(filepath):
    """
    Check if file exists and is not empty

    Args:
        filepath: Path to file

    Returns:
        Boolean
    """
    if not os.path.exists(filepath):
        return False

    if os.path.getsize(filepath) == 0:
        return False

    return True
