import datetime
import logging
import os
import snscrape.modules.twitter as sntwitter
import sqlite3 
import urllib.request

''' INTRODUCTION
--- OVERVIEW --- 
This program uses snscrape to archive all publicly available tweets from a list of Twitter useusersr, along with: 
*Entire chains of retweets, quoted tweets replied to tweets 
*Media (videos, photos, etc.)

snscrape is available here: https://github.com/JustAnotherArchivist/snscrape


--- INSTRUCTIONS ---
1. Add the accounts you want to archive to the "TWITTER_ACCOUNT" list. Do NOT include the @ symbol
e.g., the term "jack" is valid, but "@jack" is not
2. (Optional) Run this program on a VPN or via proxy. While blocking's not been observed, it may occur
3. Wait for it to finish! Depending on the number of tweets to archive, your device's hardware, your internet connection, etc; this script may take up to a day to run
4. (Optional) When finished and to save space, compress the DB

- Note on media (videos, photos, etc) -  
To save media in sqlite, it *must* be converted to a BLOB - a Binary Large OBject (BLOB).
To retrieve and view media that's been saved to the DB, you'll need to either:
(1) convert from a BLOB to the standard file format and save to your local file 
(2) view directly via a program like DB Browser for SQLite

Here are some functions for doing option #1, from Twilio (https://www.twilio.com/blog/intro-multimedia-file-upload-python-sqlite3-database): 

def read_blob_data(entry_id):
  try:
    conn = sqlite3.connect('app.db')
    cur = conn.cursor()
    print("[INFO] : Connected to SQLite to read_blob_data")
    sql_fetch_blob_query = """SELECT * from uploads where id = ?"""
    cur.execute(sql_fetch_blob_query, (entry_id,))
    record = cur.fetchall()
    for row in record:
      converted_file_name = row[1]
      photo_binarycode  = row[2]
      # parse out the file name from converted_file_name
      # Windows developers should reverse "/" to "\" to match your file path names 
      last_slash_index = converted_file_name.rfind("/") + 1 
      final_file_name = converted_file_name[last_slash_index:] 
      write_to_file(photo_binarycode, final_file_name)
      print("[DATA] : Image successfully stored on disk. Check the project directory. \n")
    cur.close()
  except sqlite3.Error as error:
    print("[INFO] : Failed to read blob data from sqlite table", error)
  finally:
    if conn:
        conn.close()

def write_to_file(binary_data, file_name):
  with open(file_name, 'wb') as file:
    file.write(binary_data)
  print("[DATA] : The following file has been written to the project directory: ", file_name)

1. read the blob files
2. write to your local file
'''
# ---------------------------
# --- INITIALIZING SCRIPT ---
# ---------------------------

# --                --
# - Global Variables -
# --                --
START_TIME = datetime.datetime.now()

#twitter account to search
TWITTER_ACCOUNT = ["example1", "example2"]

 # Change this value. e.g., TWITTER_ACCOUNT = "jack"
# TWITTER_ACCOUNT = str(input("Enter twitter account handle/username: "))

#table names
TWEETS_TABLE_NAME = "tweets" 
USERS_TABLE_NAME = "users"

# Connecting/creating database. The DB file will be saved to the directory you're running this script in
conn = sqlite3.connect("tweets_archive.db") 
c = conn.cursor()
# --                       --
# - End of Global Variables -
# --                       --

'''LOGGING
# --             --
# - Logging Setup - 
# --             --
# Omitting for production - the other stats displayed are more valuable. 
# But I've left this and other commented out logging functions (e.g., 
# logging.info, logging.error, etc.) to make debugging easier
class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    green = "\x1b[1;32m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    blue = "\x1b[1;34m"
    light_blue = "\x1b[1;36m"
    purple = "\x1b[1;35m"
    reset = "\x1b[0m"

    format = "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: green + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: purple + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)
# --                    --
# - End of Logging Setup - 
# --                    --
'''
# ----------------------------------
# --- END OF INITIALIZING SCRIPT ---
# ----------------------------------


# ------------------------
# --- SCRIPT FUNCTIONS ---
# ------------------------
def create_archive_counter():
    message = "Creating global ARCHIVED_ITEMS_COUNT variable"
    # logging.info(message)
    print(message)
    global ARCHIVED_ITEMS_COUNT
    ARCHIVED_ITEMS_COUNT = 0

def create_skipped_archive_counter():
    message = "Creating global SKIPPED_ITEMS_COUNT variable"
    # logging.info(message)
    print(message)
    global SKIPPED_ITEMS_COUNT
    SKIPPED_ITEMS_COUNT = 0

def create_total_items_viewed_counter():
    message = "Creating global TOTAL_ITEMS_VIEWED variable"
    # logging.info(message)
    print(message)
    global TOTAL_ITEMS_VIEWED
    TOTAL_ITEMS_VIEWED = 0

def create_error_counter():
    message = "Creating global ERROR_COUNTER variable"
    # logging.info(message)
    print(message)
    global ERROR_COUNT
    ERROR_COUNT = 0

def create_global_vars():
    create_archive_counter()
    create_skipped_archive_counter()
    create_total_items_viewed_counter()
    create_error_counter()

def error_handling(e):
    # logger.error(e)
    global ERROR_COUNT
    ERROR_COUNT += 1

def archive_counter():
    global ARCHIVED_ITEMS_COUNT
    ARCHIVED_ITEMS_COUNT += 1 #due to this program's recursive nature, the ARCHIVED_ITEMS_COUNT printed on-screen may seem inaccurate; however, it's counting correctly. You'll see scenarios where the same count number is printed multiple times - this is caused by the recursion

def skipped_archive_counter():
    global SKIPPED_ITEMS_COUNT
    SKIPPED_ITEMS_COUNT += 1
    return 

def total_items_viewed_counter():
    global TOTAL_ITEMS_VIEWED
    TOTAL_ITEMS_VIEWED = ARCHIVED_ITEMS_COUNT + SKIPPED_ITEMS_COUNT

def initialize_database():
    # TWEETS TABLE
    message = "Creating tweets table"
    # logging.info(message)
    print(message)
    c.execute('''
    CREATE TABLE IF NOT EXISTS tweets
    (
    [tweet_id] INTEGER PRIMARY KEY, 
    [tweet_user_name] TEXT, 
    [tweet_datetime] TEXT,
    [tweet_content] TEXT, 
    [tweet_media_content_blob] TEXT,
    [tweet_latitude] REAL,
    [tweet_longitude] REAL,
    [tweet_conversation_id] INTEGER,
    [tweet_hashtags] TEXT,
    [tweet_like_count] INTEGER,
    [tweet_language] TEXT,
    [tweet_media_filename] TEXT,
    [tweet_media_duration] TEXT,
    [tweet_media_views] INTEGER,
    [tweet_media_url] TEXT,
    [tweet_mentioned_users] TEXT,
    [tweet_place_full_name] TEXT,
    [tweet_place_name] TEXT,
    [tweet_place_type] TEXT,
    [tweet_place_country] TEXT,
    [tweet_place_country_code] TEXT,
    [tweet_quote_count] INTEGER,
    [tweet_quoted_tweet_id] INTEGER,
    [tweet_replied_to_tweet_id] INTEGER,
    [tweet_reply_count] INTEGER, 
    [tweet_retweet_count] INTEGER, 
    [tweet_retweeted_tweet_id] INTEGER,
    [tweet_source_app] TEXT,
    [tweet_url] TEXT,
    [tweet_user_id] INTEGER, 
    UNIQUE (tweet_id),
    FOREIGN KEY(tweet_user_id) REFERENCES users(user_id)
    FOREIGN KEY(tweet_quoted_tweet_id) REFERENCES tweets(tweet_id)
    FOREIGN KEY(tweet_replied_to_tweet_id) REFERENCES tweets(tweet_id)
    FOREIGN KEY(tweet_retweeted_tweet_id) REFERENCES tweets(tweet_id)
    )''')
    conn.commit()
    message = "Created tweets table"
    # logging.info(message)
    print(message)
    # END OF TWEETS TABLE

    # USERS TABLE
    message = "Creating users table"
    # logging.info(message)
    print(message)
    c.execute('''
    CREATE TABLE IF NOT EXISTS users
    (
    [user_id] INTEGER PRIMARY KEY,
    [user_username] TEXT,
    [user_display_name] TEXT,
    [user_description] TEXT,
    [user_verified] TEXT,
    [user_account_datetime_created] TEXT,
    [user_followers_count] INTEGER,
    [user_friends_count] INTEGER,
    [user_status_count] INTEGER,
    [user_favorites_count] INTEGER,
    [user_listed_count] INTEGER,
    [user_location] TEXT,
    [user_media_count] INTEGER,
    [user_account_protected] TEXT,
    [user_linked_url] TEXT,
    [user_profile_picture] TEXT,
    [user_profile_banner_picture] TEXT,
    [user_label] TEXT,
    [user_twitter_url] TEXT,
    UNIQUE (user_id)
    )''')
    conn.commit()
    message = "Created users table"
    # logging.info(message)
    print(message)
    # END OF USERS TABLE

def download_media(media_url, filename): 
    try:
        urllib.request.urlretrieve(media_url, filename) #media is named username_tweet_id.[filetype]
    except:
        return None # if the media no longer exists, return None (which results in no file being created locally)

    return urllib.request.urlretrieve(media_url, filename) #media is named username_tweet_id.[filetype]

def convert_to_binary_data(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData

def get_stats():
    elapsed_time = datetime.datetime.now() - START_TIME
    elapsed_time_mins = elapsed_time.total_seconds()
    saved_per_second = round((ARCHIVED_ITEMS_COUNT/elapsed_time_mins), 1)
    attempted_saved_per_second = round((TOTAL_ITEMS_VIEWED/elapsed_time_mins), 1)
    total_items_viewed_counter()
    return elapsed_time, saved_per_second, attempted_saved_per_second

def print_inserting_into_db_message(table_name, tweet_or_user_id, username, datetime_created_on):
    elapsed_time, items_archived_per_second, attempted_saved_per_second = get_stats()
    if "tweet" in table_name:
        saved_resource = "Tweet"
    elif "user" in table_name:
        saved_resource = "User"
    else:
        error_handling("Unknown data type attempted to save")
        saved_resource = "Unknown"
    print("\n>>> Inserting into " + table_name + " table:" + "\nCurrent Time:\t\t", (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), "\nElapsed Time:\t\t", elapsed_time, "\nTable:\t\t\t", table_name, "\n" + saved_resource + ":\t\t\t", tweet_or_user_id, "\nUser:\t\t\t", "@", username, "\nCreation Date:\t\t", datetime_created_on, "\n\nSaves/sec:\t\t", items_archived_per_second, "\nAttempted saves/sec:\t", attempted_saved_per_second, "\nArchived Count:\t\t", ARCHIVED_ITEMS_COUNT, "\nSkipped Count:\t\t", SKIPPED_ITEMS_COUNT, "\nTotal Count:\t\t", TOTAL_ITEMS_VIEWED)
    return

def insert_into_tweets_table(tweet_id, tweet_user_name, tweet_datetime, tweet_content, tweet_media_content_blob, tweet_latitude, tweet_longitude, tweet_conversation_id, tweet_hashtags, tweet_like_count, tweet_language, tweet_media_filename, tweet_media_duration, tweet_media_views, tweet_media_url, tweet_mentioned_users, tweet_place_full_name, tweet_place_name, tweet_place_type, tweet_place_country, tweet_place_country_code, tweet_quote_count, tweet_quoted_tweet_id, tweet_replied_to_tweet_id, tweet_reply_count, tweet_retweet_count, tweet_retweeted_tweet_id, tweet_source_app, tweet_url, tweet_user_id): 
    # logging.info("Inserting into tweet table:")
    print_inserting_into_db_message("tweets", tweet_id, tweet_user_name, tweet_datetime)
    archive_counter()
    c.execute("INSERT INTO tweets (tweet_id, tweet_user_name, tweet_datetime, tweet_content, tweet_media_content_blob, tweet_latitude, tweet_longitude, tweet_conversation_id, tweet_hashtags, tweet_like_count, tweet_language, tweet_media_filename, tweet_media_duration, tweet_media_views, tweet_media_url, tweet_mentioned_users, tweet_place_full_name, tweet_place_name, tweet_place_type, tweet_place_country, tweet_place_country_code, tweet_quote_count, tweet_quoted_tweet_id, tweet_replied_to_tweet_id, tweet_reply_count, tweet_retweet_count, tweet_retweeted_tweet_id, tweet_source_app, tweet_url, tweet_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (tweet_id, tweet_user_name, tweet_datetime, tweet_content, tweet_media_content_blob, tweet_latitude, tweet_longitude, tweet_conversation_id, tweet_hashtags, tweet_like_count, tweet_language, tweet_media_filename, tweet_media_duration, tweet_media_views, tweet_media_url, tweet_mentioned_users, tweet_place_full_name, tweet_place_name, tweet_place_type, tweet_place_country, tweet_place_country_code, tweet_quote_count, tweet_quoted_tweet_id, tweet_replied_to_tweet_id, tweet_reply_count, tweet_retweet_count, tweet_retweeted_tweet_id, tweet_source_app, tweet_url, tweet_user_id))
    conn.commit()    
    return

def insert_into_users_table(user_id, user_username, user_display_name, user_description, user_verified, user_account_datetime_created, user_followers_count, user_friends_count, user_status_count, user_favorites_count, user_listed_count, user_media_count, user_location, user_account_protected, user_linked_url, user_profile_picture, user_profile_banner_picture, user_label, user_twitter_url):
    # logging.info("Inserting into users table")
    print_inserting_into_db_message("users", user_id, user_username, user_account_datetime_created)
    archive_counter()
    c.execute("INSERT INTO users (user_id, user_username, user_display_name, user_description, user_verified, user_account_datetime_created, user_followers_count, user_friends_count, user_status_count, user_favorites_count, user_listed_count, user_media_count, user_location, user_account_protected, user_linked_url, user_profile_picture, user_profile_banner_picture, user_label, user_twitter_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (user_id, user_username, user_display_name, user_description, user_verified, user_account_datetime_created, user_followers_count, user_friends_count, user_status_count, user_favorites_count, user_listed_count, user_media_count, user_location, user_account_protected, user_linked_url, user_profile_picture, user_profile_banner_picture, user_label, user_twitter_url))
    conn.commit()  
    return

def check_if_artifact_exists_in_db(table_name, artifact_id):
    #artifact_id can be type str or int
    # logging.info("Checking if " + str(artifact_id) + " already exists in " + table_name)
    if "tweet" in table_name:
        column_name = "tweet_id"
    elif "user" in table_name:
        column_name = "user_id"
    else:
        error_handling("Table" + table_name + "does not exist")
        return False

    c.execute('''
        SELECT EXISTS (
            SELECT 1
            FROM %s
            WHERE %s = %s
        )''' %(table_name, column_name, artifact_id))
    exists = (c.fetchall())[0][0] #1 == Exists; 0 == Does not exist;

    if exists == 1:
        # logging.info(str(artifact_id) + " already exists in " + table_name)
        skipped_archive_counter()
        return True
    else:
        # logging.info(str(artifact_id) + " does NOT exist in " + table_name)
        return False

def get_media(media, tweet_or_user_id, username, media_url=None, media_filename=None):
    logging.info("Downloading media ")
    if media_url is not None:
        media_content = download_media(media_url, media_filename)
        media_duration = None
        media_views = None
    else:
        media_duration = None
        media_views = None
        media_url = None
        media_filename = None
        media_content_blob = None # blobs (as binary formats) are already quite compressed, and attempts to compress create a number of complications
        media_content = None

        if media is not None:
            media_type = str(type(media[0]))
            media_content = media[0]
            if "Video" in media_type:
                media_duration = media_content.duration
                media_views = media_content.views
                media_url = (media_content.variants)[0].url #Twitter can, but does not always, save more than one version type ("variant" in snscrape) per video/gif. We'll use the first variant, because (1) this will keep the DB simpler and (2) after testing, the first variant appears to always be the highest-quality version
                media_filename = str(username) + "_" + str(tweet_or_user_id) + ".mp4"
                media_content = download_media(media_url, media_filename) 
            elif "Photo" in media_type:
                media_url = media_content.fullUrl
                media_filename = str(username) + "_" + str(tweet_or_user_id) + ".jpg"
                media_content = download_media(media_url, media_filename)
            elif "Gif" in media_type:
                media_url = (media_content.variants)[0].url
                media_filename = str(username) + "_" + str(tweet_or_user_id) + ".mp4"
                media_content = download_media(media_url, media_filename)        

    if media_content is None:
        media_content_blob = None
    else:
        media_content_blob = convert_to_binary_data(media_content[0])
        os.remove(media_filename) #a file's not created if the media resource no longer exists
    
    return media_content_blob, media_duration, media_views, media_url, media_filename 

def archive_user(user_data):
    # logging.info("Running archive_user() function")

    user_id = user_data.id

    if check_if_artifact_exists_in_db("users", user_id) is True:
        return
    else:
        # logging.info("Data for user with ID " + str(user_id) + " has been pulled. Compiling data...")
        user_username = user_data.username
        user_display_name = user_data.displayname
        user_description = user_data.description
        user_verified = user_data.verified
        user_account_datetime_created = user_data.created
        user_followers_count = user_data.followersCount
        user_friends_count = user_data.friendsCount
        user_status_count = user_data.statusesCount
        user_favorites_count = user_data.favouritesCount
        user_listed_count = user_data.listedCount
        user_media_count = user_data.mediaCount
        user_location = user_data.location
        user_account_protected = user_data.protected
        user_linked_url = user_data.linkUrl
        user_profile_picture, _media_duration, _media_views, _media_url, _media_filename = get_media(None, None, None, user_data.profileImageUrl, user_username+"_profile_picture")
        user_profile_banner_picture, _media_duration, _media_views, _media_url, _media_filename = get_media(None, None, None, user_data.profileBannerUrl, user_username+"_profile_banner_picture")
        user_label = user_data.label
        user_twitter_url = user_data.url
        insert_into_users_table(user_id, user_username, user_display_name, user_description, user_verified, user_account_datetime_created, user_followers_count, user_friends_count, user_status_count, user_favorites_count, user_listed_count, user_media_count, user_location, user_account_protected, user_linked_url, user_profile_picture, user_profile_banner_picture, user_label, user_twitter_url)
        
def archive_tweet(original_tweet_id=None, new_tweet_id=None, tweet=None):
    # this utilizes recursion to archive the entire chain of retweets, quoted tweets and replied to tweets
    
    # if tweet exists, don't save it again
    if new_tweet_id is not None:
        _tmp_tweet_id = new_tweet_id
    elif tweet is not None:
        _tmp_tweet_id = tweet.id
    
    if check_if_artifact_exists_in_db("tweets", _tmp_tweet_id) is True:
        return
    else:
        if tweet is None:
            # logging.info("Checking if tweet with id " + str(new_tweet_id) + " exists on Twitter")
            try:
                _tmp_tweet = enumerate(sntwitter.TwitterTweetScraper(str(new_tweet_id)).get_items())
                for _tmp,tweet in _tmp_tweet:
                    tweet = tweet
            except Exception as e: 
                tweet_cannot_be_retrieved_message = "Tweet could not be retrieved. It's most likely been deleted."
                error_handling(tweet_cannot_be_retrieved_message)
                print(tweet_cannot_be_retrieved_message)
                return

        # placing these vars first for later parts to function
        tweet_id = tweet.id 
        tweet_user_id = tweet.user.id
        tweet_user_name = tweet.user.username
        archive_user(tweet.user)
        # logging.info("Data for tweet with ID " + str(tweet_id) + " has been pulled. Compiling data")
        # vars listed alphabetically
        tweet_content = tweet.content
        tweet_coordinates = tweet.coordinates
        if tweet_coordinates is not None:
            tweet_latitude = tweet.coordinates.latitude
            tweet_longitude = tweet.coordinates.longitude
        else:
            tweet_latitude = None
            tweet_longitude = None
        tweet_datetime = str(tweet.date)
        tweet_conversation_id = tweet.conversationId
        tweet_hashtags = tweet.hashtags 
        if tweet_hashtags is not None:
            _tweet_hashtags = ""
            for hashtag in tweet_hashtags:
                _tweet_hashtags += str(hashtag) + ", "
            tweet_hashtags = _tweet_hashtags.rstrip(', ')
            print(tweet_hashtags)
        tweet_like_count = tweet.likeCount
        tweet_language = tweet.lang
        tweet_media_content_blob, tweet_media_filename, tweet_media_duration, tweet_media_views, tweet_media_url,  = get_media(tweet.media, tweet_user_id, tweet_user_name)
        tweet_mentioned_users = tweet.mentionedUsers 
        if tweet_mentioned_users is not None:
            _mentioned_users = ""
            for user in tweet_mentioned_users:
                archive_user(user)
                _mentioned_users += str(user.username) + ", "
            tweet_mentioned_users = _mentioned_users.rstrip(', ')
        tweet_place = tweet.place
        if tweet_place is not None:
            tweet_place_full_name = tweet_place.fullName
            tweet_place_name = tweet_place.name
            tweet_place_type = tweet_place.type
            tweet_place_country = tweet_place.country
            tweet_place_country_code = tweet_place.countryCode
        else:
            tweet_place_full_name = None
            tweet_place_name = None
            tweet_place_type = None
            tweet_place_country = None
            tweet_place_country_code = None
        tweet_quote_count = tweet.quoteCount
        tweet_quoted_tweet = tweet.quotedTweet
        if tweet_quoted_tweet is not None:
            tweet_quoted_tweet_id = (tweet.quotedTweet).id
            archive_tweet(tweet_id, tweet_quoted_tweet_id)
        else:
            tweet_quoted_tweet_id = None
        tweet_replied_to_tweet_id = tweet.inReplyToTweetId
        if tweet_replied_to_tweet_id is not None:
            archive_tweet(tweet_id, tweet_replied_to_tweet_id)
        tweet_reply_count = tweet.replyCount
        tweet_retweet_count = tweet.retweetCount
        tweet_retweeted_tweet = tweet.retweetedTweet
        if tweet_retweeted_tweet is not None:
            tweet_retweeted_tweet_id = (tweet.retweetedTweet).id
            archive_tweet(tweet_id, tweet_retweeted_tweet_id)        
        else:
            tweet_retweeted_tweet_id = None
        tweet_source_app = tweet.sourceLabel
        tweet_url = tweet.url
        
        insert_into_tweets_table(tweet_id, tweet_user_name, tweet_datetime, tweet_content, tweet_media_content_blob, tweet_latitude, tweet_longitude, tweet_conversation_id, tweet_hashtags, tweet_like_count, tweet_language, tweet_media_filename, tweet_media_duration, tweet_media_views, tweet_media_url, tweet_mentioned_users, tweet_place_full_name, tweet_place_name, tweet_place_type, tweet_place_country, tweet_place_country_code, tweet_quote_count, tweet_quoted_tweet_id, tweet_replied_to_tweet_id, tweet_reply_count, tweet_retweet_count, tweet_retweeted_tweet_id, tweet_source_app, tweet_url, tweet_user_id)     

def main():
    create_global_vars()
    initialize_database()
    for account in TWITTER_ACCOUNT:
        for _tmp,tweet in enumerate(sntwitter.TwitterSearchScraper('''from:%s include:nativeretweets''' %account).get_items()):
            archive_tweet(None, None, tweet)

# -------------------------------
# --- END OF SCRIPT FUNCTIONS ---
# -------------------------------

main()
