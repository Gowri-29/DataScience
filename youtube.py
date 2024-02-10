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
        data = dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i['id'],
                Subscription_Count=i['statistics']['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                Total_Video=i['statistics']['videoCount'])
    return data

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


#creating playlistDF    

for i in Channel.find({},{'_id':0,"Playlist":1}):
    Playlist_data.append(i["Playlist"])

if Playlist_data:  # Check if Playlist_data list is not empty
    Playlist_DF = pd.concat([pd.DataFrame(inner_list) for inner_list in Playlist_data], ignore_index=True)
else:
    Playlist_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available



#creating commentsDF
    
for i in Channel.find({},{'_id':0,"Comments":1}):
    Comments_data.append(i["Comments"])

if Comments_data:  # Check if Comments_data list is not empty
    Comments_DF = pd.concat([pd.DataFrame(inner_list) for inner_list in Comments_data], ignore_index=True)
else:
    Comments_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available


#creating videosDF
for i in Channel.find({},{'_id':0,"Videos":1}):
    Videos_data.append(i["Videos"])

if Videos_data:  # Check if Videos_data list is not empty
    Videos_DF = pd.concat([pd.DataFrame(inner_list) for inner_list in Videos_data], ignore_index=True)
    Videos_DF =Videos_DF.explode('Tags')

else:
    Videos_DF = pd.DataFrame()  # Create an empty DataFrame if no data is available



def Creating_Cha_Table_and_Uploading():
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

#streamlit displaying table:    

def show_Channel():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    channel_sql = pd.read_sql_query('SELECT * FROM channel', myconnection)

# Display the data in a Streamlit table
    st.table(channel_sql)


def show_Playlist():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    Playlist_Sql=pd.read_sql_query('select * from Playlist',myconnection)
    st.table(Playlist_Sql)   


def show_Comments():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    Comments_Sql=pd.read_sql_query('select * from Comments',myconnection)
    st.table(Comments_Sql)

def show_Videos():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    Videos_Sql=pd.read_sql_query('select * from videos',myconnection)
    st.table(Videos_Sql)


#steamlit 

with st.sidebar:
    st.title(":blue[YOUTUDE DATA HARVESTING AND WAREHOUSE PROJECT]")
    st.header(":violet[PROJECT METHODOLOGY]")
    st.caption(":red[Data Collection from Youtube (Python,API Integration)]")
    st.caption(":red[Data Storage (Mongodb)]")
    st.caption(":red[Data Transfer (Mongodb to Sql)]")
    st.caption(":red[Data Analysis (Sql)]")

Channel_Id = st.text_input("Enter the Channel Id(e.g: UCHE7CG-****************)")

if st.button("Collect and Store data"):
    Channel_ids=[]
    mydb = client['Youtube_Data']
    Channel=mydb["Channel"]
    for channel_data in Channel.find({},{"_id":0,"Channel":1}):
        Channel_ids.append(channel_data["Channel"]["Channel_Id"])

    if Channel_Id in Channel_ids:
        st.success("Channel Details already exists")
    else:
        insert= Channel_input(Channel_Id)
        st.success("Uploading Channel Details Successful")
    

if st.button("Migrate to Sql"):
    Table = Creating_Table_and_Uploading_SQL()
    st.success("Migration Successful")

Show_Table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","COMMENTS","VIDEOS"))

try:
    if Show_Table == "CHANNELS":
        show_Channel()

    elif Show_Table == "PLAYLISTS":
        show_Playlist()

    elif Show_Table =="COMMENTS":
        show_Comments()

    elif Show_Table =="VIDEOS":
        show_Videos()

except:
    st.warning(":red[No data available]")
#SQL CONNECTIONS:
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
cur = myconnection.cursor()


question = st.selectbox("Select Your Questions",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question =="1. What are the names of all the videos and their corresponding channels?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    try:
        Sql=pd.read_sql_query('select distinct title as videos , channel_name as Channels from videos',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")    


elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()
    try:
        Sql=(pd.read_sql_query('SELECT Channel_Name as Channels, total_video AS Video_Count FROM channel ORDER BY total_video DESC limit 1;',myconnection))
        st.table(Sql)
    except:    
        st.warning("No data available")

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct Channel_name as Channels, views from videos order by views desc limit 10;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available") 


elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct title as Video_Name, comments as No_comments from videos where comments is not null;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")  


elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct channel_name as Channels, title as videos ,likes from videos order by likes desc limit 10;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")       

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct title as videos , likes, dislikes from videos;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")


elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct channel_name as Channels, channel_views as total_view from channel;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available") 

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('''SELECT distinct channel_name as channels,title as videos FROM videos 
                            WHERE LEFT(Published_Date, 4) = '2022';''',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available") 


elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
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

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Youtube_Data")
    cur = myconnection.cursor()

    try:
        Sql=pd.read_sql_query('select distinct channel_name as Channels, comments, title as Videos from videos order by comments desc limit 1;',myconnection)
        st.table(Sql)
    except:
        st.warning("No data available")                