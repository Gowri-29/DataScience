from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st

def Api_connect():
    Api_Id = "AIzaSyAr_EdUI8aNfx6Q2gMR31fHMENYLuFEZzI"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey = Api_Id)

    return youtube

youtube = Api_connect()


#gettting  channel details:
def get_channel_detalis(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    for i in response['items']:
        ch_data = dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i['id'],
                Subscription_Count=i['statistics']['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                Total_Video=i['statistics']['videoCount'])
    return ch_data

#getting video :
def get_videos_ids(channel_id):
    video_Ids = []
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_variable=None
    while True:
        response1 = youtube.playlistItems().list(part="snippet",playlistId=Playlist_Id,
                                maxResults=50,pageToken=next_page_variable).execute()
        for i in range(len(response1['items'])):
            video_Ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_variable=response1.get('nextPageToken')

        if next_page_variable is None:
            break
    return video_Ids


#getting details of particular video:
def get_video_details(Video_Ids):
    Video_Data=[]
    for video_id in Video_Ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id)

        response = request.execute()
        
        for item in response['items']:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags',None),
                        Description=item['snippet']['description'],
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics']['viewCount'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Comments=item['statistics'].get('commentCount'),
                        Likes=item['statistics'].get('likeCount', 0),
                        Dislikes =item['statistics'].get('dislikeCount', 0),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption'])
            
            Video_Data.append(data)
    return Video_Data



# getting the comments details:
def get_comment_details(Video_Ids):
    Comment_data=[]
    try:
        for video_id in Video_Ids:
            request=youtube.commentThreads().list(
                part="snippet",videoId=video_id,
                maxResults=50)
            
            response=request.execute()

            for item in response['items']:
                Data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comments=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])  
                
                Comment_data.append(Data)
    except:
        pass    
    return Comment_data



# GETTING PLAYLIST DETAILS
def get_playlists_details(channel):
    Playlist_Details=[]
    next_page_token = None
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel,
                maxResults=50,
                pageToken=next_page_token
            )
        response = request.execute()
        
        for item in response['items']:
            Data = dict(
                    Playlist_Id = item['id'],
                    Channel_Id = item['snippet']['channelId'],
                    Title =item['snippet']['title'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt =item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount']
                )
            Playlist_Details.append(Data)
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return Playlist_Details


#uploading the data to mongoDB:
client = pymongo.MongoClient("mongodb://localhost:27017")
mydb = client["Youtube_Data"]

def Channel_input(channel):
    Channel_details = get_channel_detalis(channel)
    Vid_Ids = get_videos_ids(channel)
    playlistDetails = get_playlists_details(channel)
    Video_details = get_video_details(Vid_Ids)
    comment_data = get_comment_details(Vid_Ids)

    Channel = mydb["Channel"]
    Channel.insert_one({"Channel":Channel_details,"Playlist":playlistDetails,"Comments":comment_data,"Videos":Video_details})


# Table creation:
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903')
cur = myconnection.cursor()

try:
    cur.execute("create database Youtube_Data")
except:
    pass 

myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
cur = myconnection.cursor()


mydb = client['Youtube_Data']
Channel=mydb["Channel"]
Channel_data= []
Playlist_data=[]
Comments_data=[]
Videos_data=[]

#creating channelDf
for i in Channel.find({},{'_id':0,"Channel":1}):
    Channel_data.append(i["Channel"])

if Channel_data:  # Check if Channel_data list is not empty
    Channel_DF = pd.DataFrame(Channel_data)
else:
    Channel_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available

if not Channel_DF.empty:
    pass
else:
    st.warning("No data available for channels.") 


#creating playlistDF    

for i in Channel.find({},{'_id':0,"Playlist":1}):
    Playlist_data.append(i["Playlist"])

if Playlist_data:  # Check if Playlist_data list is not empty
    Playlist_DF = pd.concat([pd.DataFrame(inner_list) for inner_list in Playlist_data], ignore_index=True)
else:
    Playlist_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available

if not Playlist_DF.empty:
    pass
else:
    st.warning("No data available for playlists.")    


#creating commentsDF
    
for i in Channel.find({},{'_id':0,"Comments":1}):
    Comments_data.append(i["Comments"])

if Comments_data:  # Check if Comments_data list is not empty
    Comments_DF = pd.concat([pd.DataFrame(inner_list) for inner_list in Comments_data], ignore_index=True)
else:
    Comments_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available

if not Comments_DF.empty:
    pass
else:
    st.warning("No data available for comments.")

#creating videosDF
for i in Channel.find({},{'_id':0,"Videos":1}):
    Videos_data.append(i["Videos"])

if Videos_data:  # Check if Videos_data list is not empty
    Videos_DF = pd.concat([pd.DataFrame(inner_list) for inner_list in Videos_data], ignore_index=True)
    Videos_DF =Videos_DF.explode('Tags')

else:
    Videos_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available

if not Videos_DF.empty:
    pass
else:
    st.warning("No data available for videos.")


def Creating_Cha_Table_and_Uploading():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = client["Youtube_Data"]
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    drop_query = " drop table if exists Channel"
    cur.execute(drop_query)
    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(Channel_DF.columns, Channel_DF.dtypes))

    sql_create_table = f"CREATE TABLE IF NOT EXISTS Channel ({columns});"
    Channel = sql_create_table.replace("float64","float").replace("category","text").replace("int64","int")
    cur.execute("create table Channel(Channel_Name text,Channel_Id text,Subscription_Count int,Channel_Views int,Channel_Description text, Playlist_Id text, Total_Video int)")
    sql = "insert into Channel values(%s,%s,%s,%s,%s,%s,%s)"

    for i in range(0,len(Channel_DF)):
        cur.execute(sql,tuple(Channel_DF.iloc[i]))
        myconnection.commit()

def Creating_Pla_Table_and_Uploading():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = client["Youtube_Data"]
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    drop_query = " drop table if exists Playlist"
    cur.execute(drop_query)

    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(Playlist_DF.columns, Playlist_DF.dtypes))

    sql_create_table = f"CREATE TABLE IF NOT EXISTS Playlist ({columns});"
    Playlist = sql_create_table.replace("float64","float").replace("category","text").replace("int64","int")
    cur.execute("create table Playlist(Playlist_Id text,Channel_Id text,Title text,Channel_Name text,PublishedAt text, Video_Count int)")
    sql = "insert into Playlist values(%s,%s,%s,%s,%s,%s)"

    for i in range(0,len(Playlist_DF)):
        cur.execute(sql,tuple(Playlist_DF.iloc[i]))
        myconnection.commit()
def Creating_Com_Table_and_Uploading():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = client["Youtube_Data"]
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    drop_query = " drop table if exists Comments"
    cur.execute(drop_query)
    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(Comments_DF.columns, Comments_DF.dtypes))

    sql_create_table = f"CREATE TABLE IF NOT EXISTS Comments ({columns});"
    Comments = sql_create_table.replace("float64","float").replace("category","text").replace("int64","int")
    cur.execute("create table Comments(Comment_Id text,Video_Id text,Comments text,Comment_Author text,Comment_Published_Date text)")
    sql = "insert into Comments values(%s,%s,%s,%s,%s)"

    for i in range(0,len(Comments_DF)):
        cur.execute(sql,tuple(Comments_DF.iloc[i]))
        myconnection.commit()

def Creating_Vid_Table_and_Uploading():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = client["Youtube_Data"]
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    drop_query = " drop table if exists Videos"
    cur.execute(drop_query)

    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(Videos_DF.columns, Videos_DF.dtypes))

    sql_create_table = f"CREATE TABLE IF NOT EXISTS Videos ({columns});"
    Videos = sql_create_table.replace("float64","float").replace("category","text").replace("int64","int")
    cur.execute("create table Videos(Channel_Name text,Channel_Id text,Video_Id text,Title text,Tags text,Description text,Published_Date text,Duration text,Views int,Thumbnail text,Comments int,Likes int,Dislikes int,Favorite_Count int,Definition text,Caption_Status text)")
    sql = "insert into Videos values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    for i in range(0,len(Videos_DF)):
        cur.execute(sql,tuple(Videos_DF.iloc[i]))
        myconnection.commit()

def Creating_Table_and_Uploading_SQL():
    Creating_Cha_Table_and_Uploading()
    Creating_Pla_Table_and_Uploading()
    Creating_Com_Table_and_Uploading()
    Creating_Vid_Table_and_Uploading()


#steamlit 

st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSE PROJECT]")
        
Channel_Id = st.text_input(":violet[Enter the Youtube Channel ID]")

        
if st.button("Collection of Data"):
    Channel_ids=[]
    mydb = client['Youtube_Data']
    Channel=mydb["Channel"]
    for channel_data in Channel.find({},{"_id":0,"Channel":1}):
        Channel_ids.append(channel_data["Channel"]["Channel_Id"])

    if Channel_Id in Channel_ids:
        st.success("Channel Details already exists")
    else:
        Channel_input(Channel_Id)
        st.balloons()
        
        
if st.button("Migrate to Sql"):
    Creating_Table_and_Uploading_SQL()
    st.balloons()

     
                
#SQL CONNECTIONS:
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
cur = myconnection.cursor()

Show_Table=st.selectbox("SELECT THE QUESTIONS FOR VIEW",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


if Show_Table =="1. What are the names of all the videos and their corresponding channels?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    try:
        Sql=pd.read_sql_query('select distinct title as videos , channel_name as Channels from videos',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")    


elif Show_Table =="2. Which channels have the most number of videos, and how many videos do they have?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    try:
        Sql=(pd.read_sql_query('SELECT Channel_Name as Channels, total_video AS Video_Count FROM channel ORDER BY total_video DESC limit 1;',myconnection))
        st.table(Sql)
    except:    
        st.warning("No data available")

elif Show_Table =="3. What are the top 10 most viewed videos and their respective channels?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct Channel_name as Channels, views from videos order by views desc limit 10;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available") 


elif Show_Table =="4. How many comments were made on each video, and what are their corresponding video names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct title as Video_Name, comments as No_comments from videos where comments is not null;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")  


elif Show_Table =="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct channel_name as Channels, title as videos ,likes from videos order by likes desc limit 10;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")       

elif Show_Table =="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct title as videos , likes, dislikes from videos;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")


elif Show_Table =="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct channel_name as Channels, channel_views as total_view from channel;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available") 

elif Show_Table =="8. What are the names of all the channels that have published videos in the year 2022?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('''SELECT distinct channel_name as channels,title as videos FROM videos 
                            WHERE LEFT(Published_Date, 4) = '2022';''',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available") 


elif Show_Table =="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query ('''SELECT Channel_Name as Channels, AVG(Normalized_Minutes) AS Avg_Duration FROM 
                            (SELECT Channel_Name,FLOOR(SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'T', -1), 'M', 1)) + 
                            SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'T', -1), 'M', -1) / 60 AS Normalized_Minutes FROM Videos) AS NormalizedDurations 
                            GROUP BY Channel_Name;''',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")       

elif Show_Table =="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct channel_name as Channels, comments, title as Videos from videos order by comments desc limit 1;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")                
