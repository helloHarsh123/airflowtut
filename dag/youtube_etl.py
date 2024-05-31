import pandas as pd
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv
import googleapiclient.discovery

load_dotenv()
def process_comments(response_items):
    comments = []
    for comment in response_items:
            author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
            comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
            publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
            comment_info = {'author': author, 
                    'comment': comment_text, 'published_at': publish_time}
            comments.append(comment_info)
    print(f'Finished processing {len(comments)} comments.')
    return comments

def run_youtube_etl():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    # os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = 'youtube'
    api_version = 'v3'
    access_key = os.getenv('YOUTUBE_ACCESS_KEY')

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=access_key)

    video_id = 'q8q3OFFfY6c'

    request = youtube.commentThreads().list(
        part="snippet, replies",
        videoId = video_id
    )

    response = request.execute()

    comments_list = []
    while response.get('nextPageToken', None):
            request = youtube.commentThreads().list(
                part='id,replies,snippet',
                videoId=video_id,
                pageToken=response['nextPageToken']
            )
            response = request.execute()
            comments_list.extend(process_comments(response['items']))

    df = pd.DataFrame(comments_list)
    key = os.getenv('AWS_S3_KEY')
    secret = os.getenv('AWS_S3_SECRET')
    df.to_csv(f"s3://test-etl-harsh1/comments_yt.csv",
              index=False,
              storage_options={
                    "key": key,
                    "secret" : secret,
              })

    # f = open("response.json", "w")
    # json.dump(response, f)
    # f.close()
    # print(response)
run_youtube_etl()


