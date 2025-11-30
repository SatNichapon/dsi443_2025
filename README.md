            SEARCH QUERIES  
                    |
                    v
    [Collector] search_youtube_query()  
                    |
                    v
      Unique video metadata list 
                    |
                    v
    [Analyzer] analyze_single_video()    
                    |
                    v
    Gemini 2.5 Flash --> JSON output
                    |
                    v
                  Merge 
                    |
                    v
             analyze_timeline.json
