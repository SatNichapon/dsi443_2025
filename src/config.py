import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

YOUTUBE_DATA_API_KEY = os.getenv("GEMINI_API_KEY_Sho1")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY_Sho2")

if not YOUTUBE_DATA_API_KEY:
    print("WARNING: YOUTUBE_DATA_API_KEY is missing from .env")
if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY is missing from .env")

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
SRC_DIR = BASE_DIR / "src"

OUTPUT_DIR.mkdir(exist_ok=True)

PROMPTS_FILE = SRC_DIR / "prompts.yaml"
URL_LIST_FILE = OUTPUT_DIR / "target_videos.json"
FINAL_OUTPUT_FILE = OUTPUT_DIR / "analyze_timeline.json"

MODEL_NAME = "gemini-2.0-flash-lite" 

# SEARCH_QUERIES = [
#     # 2012 – ก่อตั้ง TPUSA
#     "Charlie Kirk 2012 Turning Point USA founding",
    
#     # 2016 – RNC speaker + Trump youth vote
#     "Charlie Kirk 2016 RNC convention speech youth vote Trump",
    
#     # 2017 – Kent State diaper protest
#     "Charlie Kirk Kent State diaper USA protest 2017",
    
#     # 2018 – Prove Me Wrong / White Privilege
#     "Charlie Kirk Prove Me Wrong table white privilege 2018",
    
#     # 2018 – Politicon debate Cenk Uygur Hasan Piker
#     "Charlie Kirk Cenk Uygur Hasan Piker Politicon 2018 debate",
    
#     # 2019 – Charlie Kirk Show launch
#     "Charlie Kirk Show launch 2019 first episode",
    
#     # 2019 – Socialism Sucks tour / MAGA hats
#     "Charlie Kirk Socialism Sucks campus tour MAGA hats 2019",
    
#     # 2019 – Groyper Q&A confrontations
#     "Charlie Kirk Groyper Q and A confrontation 2019",
    
#     # 2020 – China Lied / lockdown tour
#     "Charlie Kirk China Lied tour 2020 anti lockdown",
    
#     # 2021 – Exposing Critical Racism / CRT in schools
#     "Charlie Kirk Exposing Critical Racism tour CRT schools 2021",
    
#     # 2022 – Educate Don't Mandate medical freedom
#     "Charlie Kirk Educate Dont Mandate tour 2022 medical freedom",
    
#     # 2022 – Screaming Student Penn State
#     "Charlie Kirk Screaming Student Penn State 2022 viral clip",
    
#     # 2022–2023 – Table flip Only Two Genders
#     "Charlie Kirk table flip Only Two Genders sign 2023",
    
#     # 2023 – UC Davis riot event
#     "Charlie Kirk UC Davis event 2023 riot protest police",
    
#     # 2023 – Abortion is Murder debates
#     "Charlie Kirk abortion is murder debate 2023",
    
#     # 2023 – Party of Baal theology pivot
#     "Charlie Kirk Party of Baal theology clip 2023",
    
#     # 2023–2024 – Seven Mountain Mandate pastors
#     "Charlie Kirk Seven Mountain Mandate mobilize pastors 2024",
    
#     # 2024 – Jubilee Surrounded episode
#     "Charlie Kirk Jubilee Surrounded 2024 full episode",
    
#     # 2024 – Haitian migrants Brainwashed tour
#     "Charlie Kirk Brainwashed tour Haitian migrants 2024",
    
#     # 2025 – National Bitcoin Reserve advocacy
#     "Charlie Kirk National Bitcoin Reserve 2025 speech",
    
#     # 2025 – Utah Valley University assassination debate
#     "Charlie Kirk Utah Valley University debate assassination 2025",
    
#     # 2025 – Presidential Medal of Freedom posthumous
#     "Charlie Kirk Presidential Medal of Freedom 2025 ceremony"
# ]

SEARCH_QUERIES = [
    "Charlie Kirk Fox Business 2011",
    "Charlie Kirk Turning Point USA TPUSA 2012",
    "Charlie Kirk RNC speech 2016",
    "Charlie Kirk Kent State diaper protest 2017",
    "Charlie Kirk Prove Me Wrong white privilege 2018",
    "Charlie Kirk Politicon debate Cenk Hasan 2018",
    "Charlie Kirk Show launch 2019",
    "Charlie Kirk Socialism Sucks tour 2019",
    "Charlie Kirk Groypers Q&A confrontation 2019",
    "Charlie Kirk China Lied tour lockdown 2020",
    "Charlie Kirk Critical Race Theory CRT tour 2021",
    "Charlie Kirk Educate Dont Mandate tour 2022",
    "Charlie Kirk Screaming Student Penn State 2022",
    "Charlie Kirk Only Two Genders table flip 2023",
    "Charlie Kirk UC Davis protest riot 2023",
    "Charlie Kirk abortion is murder debate 2023",
    "Charlie Kirk Party of Baal theology 2023",
    "Charlie Kirk Seven Mountain Mandate 2024",
    "Charlie Kirk Jubilee Surrounded 2024",
    "Charlie Kirk Brainwashed Haitian migrants 2024",
    "Charlie Kirk Bitcoin Reserve 2025",
    "Charlie Kirk Utah Valley University debate 2025",
    "Charlie Kirk Presidential Medal of Freedom 2025"
]

# -------------------------------------------------------------------------------------------------- sensitive config
MAX_VIDEOS_PER_QUERY = 20
MAX_WORKERS_ANALYSIS = 1
DELAY_SECONDS = 50
# -------------------------------------------------------------------------------------------------- sensitive config

def load_prompt(prompt_name="influencer_brand_v1"):
    try:
        with open(PROMPTS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get(prompt_name, "")
    except FileNotFoundError:
        print("{PROMPTS_FILE}")
        return ""

PROMPT_MESSAGE = load_prompt("influencer_brand_v1")