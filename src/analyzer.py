import time
import json
import logging
import concurrent.futures
from google import genai
from google.genai import types

from . import config

logger = logging.getLogger(__name__)

def analyze_single_video(video_data: dict) -> dict | None:
    """
    Sends a single video to the Gemini 2.5 Flash model for multimodal narrative analysis.

    This function takes existing metadata (from the Collector), sends the video URL
    to Gemini, and merges the AI's analysis (Topic, Conflict, etc.) back into
    the original dictionary.

    Args:
        video_data (dict): A dictionary containing at least:
            - 'url' (str): The YouTube video URL.
            - 'title' (str): The video title (used in the prompt context).

    Returns:
        dict | None: A unified dictionary containing both the original metadata
                     and the AI-generated analysis keys. Returns None if the API call fails.
    """
    client = genai.Client(api_key=config.GEMINI_API_KEY)
    url = video_data['url']

    max_retries = 3
    base_wait_time = 30 # Seconds to wait if we hit a limit

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
                    f"Analyze this video titled: '{video_data['title']}'"
                ]
            )
            # Merge AI analysis with the metadata we already have
            ai_result = json.loads(response.text)
            
            final_record = {
                **video_data,   # Keeps url, title, publish_date
                **ai_result     # Adds topic, conflict, etc.
            }

            logger.info(f"Success: {final_record.get('topic', 'Unknown Topic')}")
            return final_record

        except Exception as e:
            error_msg = str(e)
            # Check if the error is a Rate Limit (429)
            if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                wait_time = base_wait_time * (attempt + 1) # Wait 30s, then 60s, then 90s
                logger.warning(f"Rate Limit hit on {url}. Sleeping for {wait_time}s...")
                time.sleep(wait_time)
            else:
                # If it's a real error (like 404 Not Found), fail immediately
                logger.error(f"Fatal Error on {url}: {e}")
                return None 

def run_analysis_pipeline(video_list: list[dict]) -> list[dict]:
    """
    Orchestrates the parallel analysis of a list of video objects.

    Uses a ThreadPoolExecutor to process multiple videos simultaneously while
    enforcing a rate-limit delay to adhere to API quotas.

    Args:
        video_list (list[dict]): A list of video metadata dictionaries to analyze.

    Returns:
        list[dict]: A list of fully analyzed video records. Failed items are excluded.
    """
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS_ANALYSIS) as executor:
        future_to_video = {executor.submit(analyze_single_video, video): video for video in video_list}
        
        for future in concurrent.futures.as_completed(future_to_video):
            data = future.result()
            if data:
                results.append(data)
            # Rate limit buffer
            time.sleep(config.DELAY_SECONDS)
            
    return results