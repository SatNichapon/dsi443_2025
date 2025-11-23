import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- API KEYS ---
# MAPPING FIX: collector.py looks for 'GOOGLE_CLOUD_API_KEY', so we assign your env var to that.
YOUTUBE_DATA_API_KEY = os.getenv("YOUTUBE_DATA_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Validation
if not YOUTUBE_DATA_API_KEY:
    print("WARNING: YOUTUBE_DATA_API_KEY is missing from .env")
if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY is missing from .env")

# --- PROJECT PATHS ---
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
SRC_DIR = BASE_DIR / "src"

OUTPUT_DIR.mkdir(exist_ok=True)

PROMPTS_FILE = SRC_DIR / "prompts.yaml"
URL_LIST_FILE = OUTPUT_DIR / "target_videos.json"
FINAL_OUTPUT_FILE = OUTPUT_DIR / "analyze_timeline.json"

# --- AI MODEL SETTINGS ---
MODEL_NAME = "gemini-2.0-flash-lite" 

# --- SEARCH SETTINGS ---
SEARCH_QUERIES = [
    "Charlie Kirk Brainwashed Tour debate",
    "Charlie Kirk Prove Me Wrong",
    "Charlie Kirk campus argument",
    "Charlie Kirk Q&A session",
    "Charlie Kirk assassination aftermath"
]
MAX_VIDEOS_PER_QUERY = 1

# --- ANALYSIS SETTINGS ---
MAX_WORKERS_ANALYSIS = 1
DELAY_SECONDS = 10

# --- PROMPTS ---
def load_prompt(prompt_name="charlie_v1"):
    """
    Loads a specific prompt text from the external YAML configuration file.

    Allows for cleaner code and easier editing of large text blocks without
    modifying the Python source directly.

    Args:
        prompt_name (str): The key to look for in prompts.yaml (default: "charlie_v1").

    Returns:
        str: The content of the prompt. Returns an empty string if the file is missing
            or the key is not found.
    """
    try:
        with open(PROMPTS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get(prompt_name, "")
    except FileNotFoundError:
        print(f"Error: Could not find prompts file at {PROMPTS_FILE}")
        return ""

# Load the prompt so other modules can import it
PROMPT_MESSAGE = load_prompt("charlie_v1")