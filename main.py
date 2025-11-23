import os
import json
import logging

from src import config
from src.collector import run_collection_pipeline
from src.analyzer import run_analysis_pipeline

# --- LOGGING SETUP ---
# Create a custom logger that saves to both File and Console
log_file_path = config.OUTPUT_DIR / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='w', encoding='utf-8'), # Log to file
        logging.StreamHandler() # Log to console
    ]
)
logger = logging.getLogger(__name__)

def load_cached_videos() -> list[dict] | None:
    """
    Checks for and loads an existing JSON file containing collected video data.

    This serves as a 'checkpoint' mechanism. If the collection phase was already
    run, this function loads the data to skip re-querying the YouTube API.

    Returns:
        list[dict] | None: A list of video dictionaries if valid data exists, 
                           otherwise None (if file missing or format is old).
    """
    if os.path.exists(config.URL_LIST_FILE):
        with open(config.URL_LIST_FILE, "r") as f:
            data = json.load(f)
            if data and isinstance(data[0], dict):
                return data
            else:
                logger.warning("Old data format detected. Re-collecting.")
                return None
    return None

def save_json(data: list | dict, filepath: str):
    """
    Saves data to a JSON file with pretty printing.

    Args:
        data (list | dict): The Python object to serialize (usually a list of video dicts).
        filepath (str): The complete system path where the file should be written.
    """
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved data to '{filepath}'")

def main():
    """
    The main entry point for the Pipeline.

    Workflow:
    1. Setup: Initialize logging and directories.
    2. Phase 1 (Collection): Check for cached video data. If not found or user declines,
       run the Collector to search YouTube and fetch metadata (Title, Date, URL).
    3. Phase 2 (Analysis): Pass the video data to the Analyzer to extract narrative
       insights using Gemini 2.5 Flash.
    4. Phase 3 (Finalize): Save the combined dataset to the output folder.
    """
    logger.info("STARTING PIPELINE")
    logger.info(f"Output Directory: {config.OUTPUT_DIR}")
    logger.info(f"Log File: {log_file_path}")

    # --- PHASE 1: COLLECTION ---
    video_data = load_cached_videos()
    
    if video_data:
        logger.info(f"Found existing list with {len(video_data)} videos.")
        choice = input("Do you want to use this list? (y/n): ").strip().lower()
        if choice != 'y':
            video_data = None

    if not video_data:
        logger.info("Starting fresh collection...")
        video_data = run_collection_pipeline() # run collection
        save_json(video_data, config.URL_LIST_FILE)
    
    if not video_data:
        logger.error("No videos found. Exiting.")
        return

    # --- PHASE 2: ANALYSIS ---
    logger.info(f"Starting analysis on {len(video_data)} videos...")
    analysis_results = run_analysis_pipeline(video_data) # run analysis

    # --- PHASE 3: FINALIZE ---
    save_json(analysis_results, config.FINAL_OUTPUT_FILE)
    logger.info(f"PROJECT COMPLETE. Analyzed {len(analysis_results)} videos.")

if __name__ == "__main__":
    main()