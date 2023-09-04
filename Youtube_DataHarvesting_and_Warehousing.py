import pandas as pd
import mysql.connector as sql
from sqlalchemy import create_engine
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import streamlit as st
from googleapiclient.discovery import *
from datetime import datetime
import isodate


#Connecting to MongoDB Atlas
uri = "mongodb+srv://arvindjawahar:hIk8CxRARz1USGVT@cluster0.ooan6hu.mongodb.net/?retryWrites=true&w=majority"
myclient = MongoClient(uri, server_api=ServerApi('1'))
db = myclient["youtube_DB"]
information = db.youtube_DB

#Connecting with MySQL DataBase
db_url = "mysql+mysqlconnector://root:Qwerty@09876@localhost:3306/youtube_data"
engine = create_engine(db_url)
mydb = sql.connect(host="127.0.0.1",
                  user="root",
                  password="Qwerty@09876",
                  database="youtube_data")
cursor = mydb.cursor()

#Streamlit setup
st.set_page_config(page_title= "YouTube Data Harvesting and Warehousing | by ArvindJawahar",
                   layout= "wide")
st.title(":red[YouTube] Data Harvesting and Warehousing | by ArvindJawahar")
tab1, tab2, tab3, tab4 = st.tabs(["Home", "DataHarvesting", "Warehousing", "Result"])

# Define session state variable for tab navigation
if "current_tab" not in st.session_state:
    st.session_state.current_tab = "Home"

#Connection with API Key
api_key = "AIzaSyAcBmh-SggHfxcSctshnPOhiQtkpXYQeX4"
youtube = build("youtube", "v3", developerKey = api_key)

#Function to get Channel details
def get_channel_details(youtube, channel_id):
    ch_data = []
    request = youtube.channels().list(part='snippet,contentDetails,statistics',
                                      id=channel_id)
    try:
        response = request.execute()
        items = response.get('items', [])

        for item in items:
            data = dict(
                channel_id=channel_id,
                channel_name=item['snippet']['title'],
                channel_playlistid=item['contentDetails']['relatedPlaylists']['uploads'],
                subscribers=item['statistics']['subscriberCount'],
                views=item['statistics']['viewCount'],
                total_videos=item['statistics']['videoCount'],
                description=item['snippet']['description'],
                country=item['snippet'].get('country'),
                published_At=item['snippet']['publishedAt'],
            )
            ch_data.append(data)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return ch_data



#Function to get Video Ids
def get_video_ids(youtube, a):
    video_ids = []
    request = youtube.playlistItems().list(part='snippet,contentDetails,id,status',
                                           playlistId = a,
                                           maxResults = 50)
    response = request.execute()

    for i in range(len(response["items"])):
        video_ids.append(response["items"][i]["contentDetails"]['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:

        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=a,
                maxResults=50,
                pageToken=next_page_token)

            response = request.execute()
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')
    return video_ids


#Function to get video details
def get_video_details(youtube, video_ids):
    video_details = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(part='snippet,contentDetails,statistics',
                                        id=','.join(video_ids[i:i + 50]))
        try:
            response = request.execute()
        except Exception as e:
            print(f"An error occurred while fetching video details: {str(e)}")
            continue  # Skip this batch and proceed with the next one

        if 'items' in response:
            for video in response["items"]:
                video_stats = dict(channel_name = video['snippet']['channelTitle'],
                                   channel_id = video['snippet']['channelId'],
                                   video_id = video['id'],
                                   title = video['snippet']['title'],
                                   tags = video['snippet'].get('tags'),
                                   thumbnail = video['snippet']['thumbnails']['default']['url'],
                                   description = video['snippet']['description'],
                                   published_date = video['snippet']['publishedAt'],
                                   duration=video.get('contentDetails', {}).get('duration', None),
                                   views = video['statistics']['viewCount'],
                                   likes = video['statistics'].get('likeCount', 0),
                                   comments = video['statistics'].get('commentCount', 0),
                                   favorite_count = video['statistics']['favoriteCount'],
                                   definition=video.get('contentDetails', {}).get('definition', None),
                                   caption_status=video.get('contentDetails', {}).get('caption', None)
                                   )
                video_details.append(video_stats)
    return video_details

#Function to get Comment Details
def get_comments_details(youtube, v):
    all_comments = []
    for video_id in v:
        try:
            comment_request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=5
            )
            comment_response = comment_request.execute()
            if 'items' in comment_response:
                for comment in comment_response['items']:
                    comment = dict(comment_id=comment['snippet']['topLevelComment']['id'],
                                   video_id=comment['snippet']['videoId'],
                                   comment_text=comment['snippet']['topLevelComment']['snippet']['textOriginal'],
                                   comment_author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                   comment_publishedAt=comment['snippet']['topLevelComment']['snippet']['publishedAt']
                                   )
                    all_comments.append(comment)


        except Exception as e:

            print(f"An error occurred while fetching comments: {str(e)}")

            continue  # Skip this video and proceed with the next one
    return all_comments

# Function to get All PlaylistIds
def playlist(youtube, channel_id):
    playlist=[]
    playlist_request = youtube.playlists().list(
         part="snippet,contentDetails",
         channelId=channel_id,
         maxResults=50
    )
    response = playlist_request.execute()


    for i in range(len(response['items'])):
        data = dict(playlist_id=response['items'][i]['id'],
                    channel_id=response['items'][i]['snippet']['channelId'],
                    playlist_title=response['items'][i]['snippet']['localized']['title'],
                    playlist_count=response['items'][i]['contentDetails']['itemCount'],
                    playlist_publishedate=response['items'][i]['snippet']['publishedAt'])
        playlist.append(data)

    next_page_token=response.get('nextPageToken')
    more_pages=True
    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            request = youtube.playlists().list(
                                            part="snippet,contentDetails",
                                            channelId=channel_id,
                                            maxResults=50,
                                            pageToken=next_page_token
            )
            response=request.execute()
            for i in range(len(response['items'])):
                data =dict(Playlist_id=response['items'][i]['id'],
                           Channel_id=response['items'][i]['snippet']['channelId'],
                           Playlist_count=response['items'][i]['contentDetails']['itemCount'],
                           Playlist_title=response['items'][i]['snippet']['localized']['title'],
                           Playlist_publishedate=response['items'][i]['snippet']['publishedAt'])
                playlist.append(data)
            next_page_token= response.get('nextPageToken')
    return playlist

#Function to merge all data
def main(channel_id):
   youtube = build("youtube", "v3", developerKey=api_key)
   CD = get_channel_details(youtube, channel_id) # Channel_details
   playlist_details = playlist(youtube,channel_id) #playlist_details
   video_ids = get_video_ids(youtube, CD[0]['channel_playlistid']) #video_ids details
   video_details = get_video_details(youtube, video_ids)  # get_video_details
   comment_details = get_comments_details(youtube, video_ids)  # get_comments_in_video

   data = {"Channel_Details": CD,
           "Playlist_Details": playlist_details,
           "Video_Details": video_details,
           "Comments_Details": comment_details}
   return data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():

        ch_name.append(i["channel_name"])
    return ch_name


#Streamlit Setup

with tab1:
    st.header(":blue[Project Title:]")
    st.subheader("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    st.header(":blue[Skills learned from This Project]")
    st.subheader("Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Managment using MongoDB (Atlas) and SQL")
    st.header(":blue[DOMAIN - SOCIAL MEDIA]")

with tab2:
    channel_id = st.text_input("Enter Channel ID below:")
    Get_data = st.button("Submit")

    if channel_id and Get_data:
        ch_details = get_channel_details(youtube, channel_id)
        if ch_details:
            st.write(":green[Channel Name is]   " + ch_details[0]["channel_name"])
            st.table(ch_details)
        else:
            st.write(":warning: Channel details not found.")


    if st.button("Upload to MongoDB"):
        with st.spinner("Uploading..."):
            youtube = build("youtube", "v3", developerKey=api_key)
            ch_details = get_channel_details(youtube, channel_id)  # Channel_details
            playlist_details = playlist(youtube, channel_id)  # playlist_details
            video_ids = get_video_ids(youtube, ch_details[0]['channel_playlistid'])  # video_ids details
            video_details = get_video_details(youtube, video_ids)  # get_video_details
            comment_details = get_comments_details(youtube, video_ids)

            collections1 = db.channel_details
            # Check if channel details already exist in the collection
            existing_channel = collections1.find_one({"channel_id": ch_details[0]["channel_id"]})
            if existing_channel:
                st.warning("Channel details already exist in MongoDB.")
            else:
                collections1.insert_many(ch_details)
                st.success("Channel details uploaded to MogoDB !!")

            collections2 = db.video_details
            # Check if video details already exist in the collection
            existing_video = collections2.find_one({"video_id": video_details[0]["video_id"]})
            if existing_video:
                st.warning("Video details already exist in MongoDB.")
            else:
                collections2.insert_many(video_details)
                st.success("Video details uploaded to MogoDB !!")

            collections3 = db.comments_details
            # Check if comment details already exist in the collection
            existing_comment = collections3.find_one({"video_id": comment_details[0]["video_id"]})
            if existing_comment:
                st.warning("Comment details already exist in MongoDB.")
            else:
                collections3.insert_many(comment_details)
                st.success("Comment details uploaded to MogoDB !!")

            collections4 = db.playlist_details
            # Check if comment details already exist in the collection
            existing_playlist = collections4.find_one({"channel_id": playlist_details[0]["channel_id"]})
            if existing_playlist:
                st.warning("Playlist details already exist in MongoDB.")
            else:
                collections4.insert_many(playlist_details)
                st.success("Playlist details uploaded to MogoDB !!")

            st.success("Upload to MogoDB successful !!")
with tab3:
    st.markdown("### Select a channel to begin Transformation to SQL")
    ch_names = channel_names()
    user_input = st.selectbox("Select channel", options=ch_names)


    def insert_into_channels_details():
        collections1 = db.channel_details

        query1= """
            INSERT INTO channels_details (channel_id, channel_name, channel_playlistid, subscribers, views, total_videos, description, country, published_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        i = collections1.find_one({"channel_name": user_input}, {'_id': 0})
        if i:
            # Convert subscribers, views, and total_videos to integers and  published_At to a MySQL DATETIME format
            subscribers = i["subscribers"]
            views = i["views"]
            total_videos = i["total_videos"]
            published_at_iso = i["published_At"]

            # Strip milliseconds, if present
            if "." in published_at_iso:
                published_at_iso = published_at_iso.split(".")[0]

            published_at = datetime.strptime(published_at_iso, "%Y-%m-%dT%H:%M:%SZ")

        data_tuple = (i["channel_id"],
                    i["channel_name"],
                    i["channel_playlistid"],
                    subscribers,
                    views,
                    total_videos,
                    i["description"],
                    i["country"],
                    published_at)
        try:
            cursor.execute(query1, data_tuple)
            mydb.commit()
        except Exception as e:
            if "Duplicate entry" in str(e):
                st.write("Duplicate entry: The data for this channel already exists in the channels_details table.")
            else:
                st.warning(f"An error occurred while inserting into 'channels_details': {str(e)}")


    # Function to insert data into the 'videos_details' table
    def insert_into_videos_details():
        collections2 = db.video_details

        query2 = """INSERT INTO videos_details (
                channel_name, channel_id, video_id, title, tags, thumbnail,
                description, published_date, duration, views, likes, comments,
                favorite_count, definition, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        for i in collections2.find({"channel_name": user_input}, {"_id": 0}):
            
            published_date = datetime.strptime(i["published_date"], "%Y-%m-%dT%H:%M:%SZ")

            # Convert the list of tags to a comma-separated string
            tags = ','.join(i["tags"]) if i["tags"] else None
            data_tuple = (
                i["channel_name"],
                i["channel_id"],
                i["video_id"],
                i["title"],
                tags,
                i["thumbnail"],
                i["description"],
                published_date,
                i["duration"],
                int(i["views"]),
                int(i["likes"]),
                int(i["comments"]),
                int(i["favorite_count"]),
                i["definition"],
                i["caption_status"]
            )
            try:
                cursor.execute(query2, data_tuple)
                mydb.commit()
            except Exception as e:
                if "Duplicate entry" in str(e):
                    st.write("Duplicate entry: The data for this channel already exists in the database.")
                else:
                    st.warning(f"An error occurred while inserting into 'videos_details': {str(e)}")

    # Function to insert data into the 'comments_details' table
    def insert_into_comments_details():
        collections2 = db.video_details
        collections3 = db.comments_details
        query3 = """
            INSERT INTO comments_details (
                comment_id, video_id, comment_text, comment_author, comment_publishedAt
            ) VALUES (%s, %s, %s, %s, %s)
        """
        video_ids = [vid["video_id"] for vid in collections2.find({"channel_name": user_input}, {"_id": 0})]

        for video_id in video_ids:

            for i in collections3.find({'video_id': video_id}, {'_id': 0}):
                try:
                    comment_publishedAt = datetime.strptime(i["comment_publishedAt"], "%Y-%m-%dT%H:%M:%SZ")

                    data_tuple = (
                        i["comment_id"],
                        i["video_id"],
                        i["comment_text"],
                        i["comment_author"],
                        comment_publishedAt
                    )
                    cursor.execute(query3, data_tuple)
                    mydb.commit()
                except Exception as e:
                    if "Duplicate entry" in str(e):
                        st.write("Duplicate entry: The data for this channel already exists in the database.")
                    else:
                        st.warning(f"An error occurred while inserting into 'comments_details': {str(e)}")

    # Function to insert data into the 'playlist_details' table
    def insert_into_playlist_details():
        collections4 = db.playlist_details
        query4 = """
            INSERT INTO playlist_details (
                playlist_id, channel_id, playlist_title,
                playlist_count, playlist_publishedate
            ) VALUES (%s, %s, %s, %s, %s)
        """
        for i in collections4.find({}, {"_id": 0, "channel_id": 1}):
            Channel_id = i["channel_id"]

            for i in collections4.find({"channel_id": Channel_id }, {"_id": 0}):
                try:
                    playlist_publishedate_str = i["playlist_publishedate"]
                    playlist_publishedate = datetime.strptime(playlist_publishedate_str, "%Y-%m-%dT%H:%M:%SZ")
                    Playlist_count = int(i["playlist_count"])

                    data_tuple = (
                        i["playlist_id"],
                        i["channel_id"],
                        i["playlist_title"],
                        Playlist_count,
                        playlist_publishedate
                    )

                    cursor.execute(query4, data_tuple)
                    mydb.commit()
                except Exception as e:
                    if "Duplicate entry" in str(e):
                        st.write("Duplicate entry: The data for this channel already exists in the database.")

                    else:
                        st.warning(f"An error occurred while inserting into 'playlist_details': {str(e)}")

    if st.button("Insert into MySQL"):
        try:

            insert_into_channels_details()
            insert_into_videos_details()
            insert_into_comments_details()
            insert_into_playlist_details()

            st.success("Transformation to MySQL Successful!!!")
        except Exception as e:
            st.warning(f"An error occurred: {str(e)}")

with tab4:
    # MYSQL Queries page
        st.write("## :black[Question are given below choose one you will get the answers]")
        question = ['1. What are the names of all the videos and their corresponding channels?',
                    '2. Which channels have the most number of videos, and how many videos do they have?',
                    '3. What are the top 10 most viewed videos and their respective channels?',
                    '4. How many comments were made on each video, and what are their corresponding video names?',
                    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                    '6. What is the total number of likes for each video, and what are their corresponding video names?',
                    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                    '8. What are the names of all the channels that have published videos in the year 2022?',
                    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                    '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
        questions = st.selectbox('Select questions', question)

        #1 question
        if questions=='1. What are the names of all the videos and their corresponding channels?':
            if st.button('Get solution'):
                cursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos_details ORDER BY channel_name""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.success("DONE", icon="✅")


        #2 question
        elif questions=='2. Which channels have the most number of videos, and how many videos do they have?':
            if st.button('Get solution'):
                cursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos FROM channels_details ORDER BY total_videos DESC""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.write("### :green[Number of videos in each channel :]")
                st.success("DONE", icon="✅")


        #3 question
        elif questions=='3. What are the top 10 most viewed videos and their respective channels?':
            if st.button('Get solution'):
                cursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views FROM videos_details ORDER BY views DESC LIMIT 10""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.write("### :green[Top 10 most viewed videos :]")
                st.success("DONE", icon="✅")


        #4 question
        elif questions=='4. How many comments were made on each video, and what are their corresponding video names?':
            if st.button('Get solution'):
                cursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                                  FROM videos_details AS a LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                                  FROM comments_details GROUP BY video_id) AS b ON a.video_id = b.video_id
                                  ORDER BY b.Total_Comments DESC""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.success("DONE", icon="✅")

        #5 question
        elif questions=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            if st.button('Get solution'):
                cursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                                            FROM videos_details
                                            ORDER BY likes DESC
                                            LIMIT 10""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.write("### :green[Top 10 most liked videos :]")
                st.success("DONE", icon="✅")


        #6 question
        elif questions=='6. What is the total number of likes for each video, and what are their corresponding video names?':
            if st.button('Get solution'):
                cursor.execute("""SELECT title AS Title, likes AS Likes_Count
                                            FROM videos_details
                                            ORDER BY likes DESC""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.success("DONE", icon="✅")


        #7 question
        elif  questions=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
            if st.button('Get solution'):
                cursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                                            FROM channels_details
                                            ORDER BY views DESC""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.write("### :green[Channels vs Views :]")
                st.success("DONE", icon="✅")


        #8 question
        elif questions=='8. What are the names of all the channels that have published videos in the year 2022?':
            if st.button('Get solution'):
                cursor.execute("""SELECT channel_name AS Channel_Name
                                            FROM videos_details
                                            WHERE published_date LIKE '2022%'
                                            GROUP BY channel_name
                                            ORDER BY channel_name""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.success("DONE", icon="✅")


        #9 question
        elif questions=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            if st.button('Get solution'):
                cursor.execute("SELECT channel_name AS Channe_Name, AVG(duration)/60 AS Average_Video_Duration FROM videos_details GROUP BY channel_name ORDER BY AVG(duration)/60 DESC")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.success("DONE", icon="✅")


        #10 question
        elif questions=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            if st.button('Get solution'):
                cursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                                            FROM videos_details
                                            ORDER BY comments DESC
                                            LIMIT 10""")
                df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                st.write(df)
                st.write("### :green[Videos with most comments :]")
                st.success("DONE", icon="✅")