import logging
import concurrent.futures
from googleapiclient.discovery import build

from . import config

logger = logging.getLogger(__name__)

def search_youtube_query(query: str, max_results: int) -> list[dict]:
    """
    Searches the YouTube Data API (v3) for a specific query and returns rich metadata.

    This function handles API pagination to retrieve the requested number of videos.
    It constructs a dictionary for each video containing the ID, URL, Title, and Date.

    Args:
        query (str): The search term to query YouTube with.
        max_results (int): The maximum number of videos to retrieve for this query.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary contains:
            - 'video_id' (str): The unique YouTube ID.
            - 'url' (str): The full watch URL.
            - 'title' (str): The video title.
            - 'publish_date' (str): ISO 8601 publication timestamp.
    """
    # construct a resource interacting with an API
    youtube = build("youtube", "v3", developerKey=config.YOUTUBE_DATA_API_KEY)
    logger.info(f"Searching for: '{query}'...")
    
    videos = []
    next_page_token = None
    
    try:
        while len(videos) < max_results:
            fetch_count = min(50, max_results - len(videos)) # youtube-data-api limit 50 items per request (single API call)
            
            request = youtube.search().list(
                q=query,
                part="id,snippet",
                type="video",
                maxResults=fetch_count,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get('items', []):
                video_data = {
                    "video_id": item['id']['videoId'],
                    "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                    "title": item['snippet']['title'],
                    "publish_date": item['snippet']['publishedAt']
                }
                videos.append(video_data)
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
        logger.info(f"Finished '{query}': Found {len(videos)} videos.")
        return videos

    except Exception as e:
        logger.error(f"Error searching '{query}': {e}")
        return set()

def run_collection_pipeline() -> list[str]:
    """
    Orchestrates parallel searches across all configured queries and removes duplicates.

    It runs multiple instances of 'search_youtube_query' concurrently using a 
    ThreadPoolExecutor. Results are deduplicated based on the 'video_id' to ensure
    we don't analyze the same video twice.

    Returns:
        list[dict]: A unique list of video metadata dictionaries collected from all queries.
    """
    all_videos_map = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(config.SEARCH_QUERIES)) as executor:
        future_to_query = {
            executor.submit(search_youtube_query, query, config.MAX_VIDEOS_PER_QUERY): query 
            for query in config.SEARCH_QUERIES
        }
        
        for future in concurrent.futures.as_completed(future_to_query):
            videos = future.result()
            for v in videos:
                # Deduplicate: If video_id is already in map, skip it
                if v['video_id'] not in all_videos_map:
                    all_videos_map[v['video_id']] = v

    return list(all_videos_map.values())