# DataScience
DS_YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

# The project consists of the following components:

# - Data extraction from YouTube using the YouTube Data API.
# - Storage of extracted data in MongoDB for flexibility and scalability.
# - Migration of data from MongoDB to a structured MySQL database for analytics.
# - Streamlit web application for interacting with the data and viewing analytics.

### Libraries Used:

- googleapiclient
- pymongo
- pymysql
- pandas
- streamlit

### Api_connect():
This function establishes a connection to the YouTube Data API using the provided API key.
1.create a API key
2.by using the API key we can connect to youtube for the extraction of the details for the youtube.

### get_channel_detalis(channel_id):
Retrieves details about a specific YouTube channel using its channel ID.
# def get_channel_detalis(channel_id):
By using this function, we can get the channel information for the particular channel(providing the channel Id as an arugument). 
1. we are going to create list, in the particular channel information we are going only extract the necessary data by using part(part="snippet,contentDetails,statistics")
2. store that information in " response"
3. By using respone, we are going to creating a dictionary for the channel details (Channel_Id,Subscription_Count,Channel_Views,Channel_Description,Playlist_Id,Total_Video)

### get_videos_ids(channel_id):
Collects the video IDs associated with a given YouTube channel.For each video we have unique id, by using the unique id we can collect the information about the videos.(i.e date, time, title,etc..)

# def get_videos_ids(channel_id):
By using this function we can collect the list of video ids.
1. Creating the empty list to store the video ids.
2. By creating the list, from that we are going to extract the necessay video Ids by using part(part='contentDetails').
3. Video_ids were created during the uploading of the video in the youtube. so we can get the unique video ids from the "upload"(response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
4. we have challenges in getting videosIds, we can only 50 as maximum result for the single, inorder to get the remains video ids we have to use key word "pageToken"
5. By using response, we are going to collect the list of video Ids. 
   
### get_video_details(Video_Ids):
Fetches details about individual videos using their IDs.
# def get_video_details(Video_Ids):
Extraction of Video details (Channel_Name,Channel_Id,Video_Id,Title,Tags,Description,Published_Date,Duration,Views,Thumbnail,Comments,Likes,Dislikes,Favorite_Count,Definition,Caption_Status)
 1. Creating the empty list to store the video Data.
2. By creating the list, from that we are going to extract the necessay video Data by using part('part="snippet,contentDetails,statistics",id=video_id').
3. By using response, we are going to collect the list of video Data. 

### get_comment_details(Video_Ids):
Collects comments associated with videos in a given list of video IDs.
# def get_comment_details(Video_Ids):
Extraction of Comment details for the respective video Ids (Comment_Id, Video_Id, Comments, Comment_Author, Comment_Published_Date)
1. Creating the empty list to store the Comments Data.
2. By creating the list, from that we are going to extract the necessay comment Data by using part('part="snippet",videoId=video_id').
4. By using response, we are going to collect the list of video Data. 
 
  
### get_playlists_details(channel):
Fetches details of playlists associated with a given channel ID.

# def get_playlists_details(channel):
  1. Creating an Playlist_Details=[]
  2. Extraction of data from respone and store it in the Playlist_Details. 
  3. Usage of pageToken - extraction of whole data from the comment section. 
   

### Channel_input(channel):
Uploads channel details, playlists, comments, and videos to MongoDB.


### Creating_Table_and_Uploading_SQL():
Creates tables in MySQL database and uploads data from MongoDB.

### creating channelDf: 
collecting the data from mongoDb and storing in data in DataFrame. If the database is empty, in order to avoid the error(empty), we have create a empty data frame and display if its empty.
### creating a table and Migrating the data from the MongoDb to Sql:

   - drop query (to remove the whole values in table , reloading the data inorder to avoid the duplicates)
   - creating a columns by using the dataframe
   - inserting the values into columns which we created.
   - we have used "explode" in pandas DF, because sql does not always list in single cell.
   
### Steamlit Web Application:

The Streamlit web application provides the following functionalities:

1. Title: Displays the title of the application.
2. Channel ID Input: Allows users to input a YouTube channel ID.
3. "Insert Data and Migrate" Button: Initiates data extraction and migration process.
4. "Migrate to SQL" Button: Triggers the migration of data to the SQL database.
5. "10 QUESTIONS FOR VIEW" Dropdown Menu: Provides a selection of questions for viewing analytics.

                
