import logging
import concurrent.futures
from googleapiclient.discovery import build
import config
from config import YOUTUBE_DATA_API_KEY , GEMINI_API_KEY

logger = logging.getLogger(__name__)

def search_youtube_query(query: str) -> list[dict]:
    youtube = build("youtube", "v3", developerKey=config.YOUTUBE_DATA_API_KEY)
    logger.info(f"Searching for: '{query}'.")

    videos = []
    seen_ids = set()
    next_page_token = None

    target_count = config.MAX_VIDEOS_PER_QUERY
    index_counter = 0  

    try:
        while len(videos) < target_count:
            fetch_count = min(50, target_count - len(videos))

            request = youtube.search().list(
                q=query,
                part="id,snippet",
                type="video",
                maxResults=fetch_count,
                pageToken=next_page_token,
                order="relevance" 

            )
            response = request.execute()

            for item in response.get("items", []):
                vid = item["id"]["videoId"]

                if vid in seen_ids:
                    continue
                seen_ids.add(vid)

                video_data = {
                    "query": query,
                    "video_id": vid,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "title": item["snippet"]["title"],
                    "publish_date": item["snippet"]["publishedAt"],
                    "description": item["snippet"]["description"],
                    "index_in_query": index_counter,   

                }
                videos.append(video_data)
                index_counter += 1 

                if len(videos) >= target_count:
                    break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break  

        logger.info(f"Finished '{query}': Found {len(videos)} videos.")
        return videos

    except Exception as e:
        logger.error(f"Error searching '{query}': {e}")
        return []

   
def run_collection_pipeline(queries: list[str] | None = None) -> list[dict]:

    if queries is None:
        queries = config.SEARCH_QUERIES

    all_videos_map: dict[str, dict] = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(queries)) as executor:
        future_to_query = {
            executor.submit(search_youtube_query, query): query
            for query in queries
        }
        
        for future in concurrent.futures.as_completed(future_to_query):
            videos = future.result() or []
            for v in videos:
                vid = v["video_id"]
                if vid not in all_videos_map:
                    all_videos_map[vid] = v  

    return list(all_videos_map.values())

