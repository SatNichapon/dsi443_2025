import time
import json
import logging
import concurrent.futures
from google import genai
from google.genai import types
from config import YOUTUBE_DATA_API_KEY , GEMINI_API_KEY
import config

logger = logging.getLogger(__name__)

def analyze_single_video(video_data: dict) -> dict | None:
    client = genai.Client(api_key=config.GEMINI_API_KEY)
    url = video_data['url']

    max_retries = 3
    base_wait_time = 30

    for attempt in range(max_retries):
        try:
            logger.info(f"Checking: {url} (Attempt {attempt + 1})")

            response = client.models.generate_content(
                model=config.MODEL_NAME,
                config=types.GenerateContentConfig(
                    system_instruction=config.PROMPT_MESSAGE,
                    response_mime_type="application/json"
                ),
                contents=[
                    types.Part.from_uri(file_uri=url, mime_type="video/mp4"),
                    f"Analyze : '{video_data['title']}'"
                ]
            )
            ai_result = json.loads(response.text)
            
            final_record = {
                **video_data,  
                **ai_result     
            }

            logger.info(f"Success: {final_record.get('topic', 'Unknown Topic')}")
            return final_record

        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                wait_time = base_wait_time * (attempt + 1) 
                logger.warning(f"Rate Limit hit on {url}. Sleeping for {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Fatal Error on {url}: {e}")
                return None 

def run_analysis_pipeline(video_list: list[dict]) -> list[dict]:
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS_ANALYSIS) as executor:
        future_to_video = {executor.submit(analyze_single_video, video): video for video in video_list}
        
        for future in concurrent.futures.as_completed(future_to_video):
            data = future.result()
            if data:
                results.append(data)
            time.sleep(config.DELAY_SECONDS)
            
    return results