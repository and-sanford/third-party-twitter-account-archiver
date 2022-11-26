import datetime
import logging
import os
import snscrape.modules.twitter as sntwitter
import sqlite3 
import urllib.request

''' INTRODUCTION
--- OVERVIEW --- 
This program uses snscrape to archive all publicly available tweets from a given Twitter user, along with: 
*Entire chains of retweets, quoted tweets replied to tweets 
*Media (videos, photos, etc.)

snscrape is available here: https://github.com/JustAnotherArchivist/snscrape


--- INSTRUCTIONS ---
1. Change "TWITTER_ACCOUNT" value to the account you're archiving. Do NOT include the @ symbol
e.g., the term "jack" is valid, but "@jack" is not
2. (Optional) Run this program on a VPN or via proxy. While blocking's not been observed, it may occur
3. Wait for it to finish! Depending on the number of tweets to archive, your device's hardware, your internet connection, etc; this script may take up to a day to run
4. (Optional) When finished, compress the DB. Doing so can reduce the DB's file size by over 90% 

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
TWITTER_ACCOUNT = "CHANGE_THIS_VALUE" # Change this value. e.g., TWITTER_ACCOUNT = "jack"

# main table names
# TWITTER_ACCOUNT, above, will be the main table's name
# it's left above for ease of finding
ORIGINAL_ACCOUNT_TWEETS = TWITTER_ACCOUNT + "_tweets"
QUOTED_TWEETS_TABLE_NAME = "quoted_tweets"
REPLIED_TO_TWEETS_TABLE_NAME = "replied_to_tweets"
RETWEETS_TABLE_NAME = "retweets"

# junction table names
# "original_tweets" are the tweet that has quoted, replied from or is retweeting a given tweet. 
# in this way, the original_tweet is relative to each quoted, replied to or retweeted tweet
QUOTED_TWEETS_JUNCTION_TABLE_NAME = "quoted_tweets__to__original_tweets"
REPLIED_TO_TWEETS_JUNCTION_TABLE_NAME = "replied_to_tweets__to__original_tweets"
RETWEETS_TO_TWEETS_JUNCTION_TABLE_NAME = "retweets__to__original_tweets"

# list/dict to initialize the databases
MAIN_TABLES = [ORIGINAL_ACCOUNT_TWEETS, QUOTED_TWEETS_TABLE_NAME, REPLIED_TO_TWEETS_TABLE_NAME, RETWEETS_TABLE_NAME]
JUNCTION_TALBES = {QUOTED_TWEETS_JUNCTION_TABLE_NAME:str(QUOTED_TWEETS_TABLE_NAME + "_id"), REPLIED_TO_TWEETS_JUNCTION_TABLE_NAME:str(REPLIED_TO_TWEETS_TABLE_NAME + "_id"), RETWEETS_TO_TWEETS_JUNCTION_TABLE_NAME:str(RETWEETS_TABLE_NAME + "_id")}

# Connecting/creating database. The DB file will be saved to the directory you're running this script in
conn = sqlite3.connect(TWITTER_ACCOUNT + "_tweets.db") 
c = conn.cursor()

# --                       --
# - End of Global Variables -
# --                       --

''' LOGGING
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
logger.addHandler(ch)'''
# ----------------------------------
# --- END OF INITIALIZING SCRIPT ---
# ----------------------------------


# ------------------------
# --- SCRIPT FUNCTIONS ---
# ------------------------
def create_tweet_counter():
    message = "Creating global TWEETS_COUNT variable"
    # logging.info(message)
    print(message)
    global TWEETS_COUNT
    TWEETS_COUNT = 0

def create_exception_counter():
    message = "Creating global ERROR_COUNTER variable"
    # logging.info(message)
    print(message)
    global ERROR_COUNT
    ERROR_COUNT = 0

def exception_handling(e):
    # logger.exception(e)
    global ERROR_COUNT
    ERROR_COUNT += 1

def tweet_counter():
    global TWEETS_COUNT
    TWEETS_COUNT += 1 #due to this program's recursive nature, the TWEETS_COUNT printed on-screen may seem inaccurate; however, it's counting correctly. You'll see scenarios where the same count number is printed multiple times - this is caused by the recursion

def initialize_databases():
    for table_name in MAIN_TABLES: 
        message = "Creating table: %s" %table_name
        # logging.info(message)
        print(message)
        c.execute('''
                CREATE TABLE IF NOT EXISTS %s
                (
                [row_id] INTEGER PRIMARY KEY, 
                [tweet_id] INTEGER, 
                [datetime] TEXT,
                [username] TEXT,
                [rendered_content] TEXT,
                [content] TEXT,
                [conversation_id] INTEGER,
                [like_count] INTEGER,
                [reply_count] INTEGER,
                [retweet_count] INTEGER,
                [quote_count] INTEGER,
                [source_label] TEXT,
                [source_url] TEXT,
                [url] TEXT,
                [user_all_data] TEXT,
                [tweet_all_data] TEXT,
                [media_url] TEXT,
                [media_filename] TEXT,
                [media_duration] TEXT,
                [media_views] TEXT,
                [media_content_blob] TEXT,
                [quoted_tweet_id] INTEGER, 
                [retweeted_tweet_id] INTEGER,
                [replied_to_tweet_id] INTEGER
                )
                ''' %table_name)
        conn.commit()

    for table_name, reference in JUNCTION_TALBES.items():
        message = "Creating junction table: %s" %table_name
        # logging.info(message)
        print(message)
        c.execute('''
                CREATE TABLE IF NOT EXISTS %s
                (
                original_tweet_id INTEGER,
                %s INTEGER,
                FOREIGN KEY(original_tweet_id) REFERENCES original_tweet_id(id),
                FOREIGN KEY(%s) REFERENCES %s(id)
                )''' % (table_name, reference, reference, reference))  
        conn.commit()

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
    tweets_saved_per_second = round((TWEETS_COUNT/elapsed_time_mins), 1)
    return elapsed_time, tweets_saved_per_second

def print_inserting_into_db_message(table_name, tweet_id, username, datetime_created_on):
    elapsed_time, tweets_saved_per_second = get_stats()
    print("\n>>> Inserting into DB:\nCurrent Time:\t", (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), "\nElapsed Time:\t", elapsed_time, "\nSaves/sec:\t", tweets_saved_per_second, "\nCount:\t\t", TWEETS_COUNT, "\nTable:\t\t", table_name, "\nTweet ID:\t", tweet_id, "\nTweet User:\t", "@",username, "\nTweet Date:\t", datetime_created_on)
    return

def insert_into_main_table(table_name, tweet_id, datetime_created_on, username, rendered_content, conversation_id, like_count, reply_count, retweet_count, quote_count, source_label, source_url, url, user_all_data, tweet_all_data, media_url, media_filename, media_duration, media_views, media_content_blob, quoted_tweet_id, retweeted_tweet_id, replied_to_tweet_id): 
    # logging.info("Inserting into DB:")
    print_inserting_into_db_message(table_name, tweet_id, username, datetime_created_on)
    c.execute("INSERT INTO %s (tweet_id, datetime, username, rendered_content, conversation_id, like_count, reply_count, retweet_count, quote_count, source_label, source_url, url, user_all_data, tweet_all_data, media_url, media_filename, media_duration, media_views, media_content_blob, quoted_tweet_id, retweeted_tweet_id, replied_to_tweet_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" %table_name, (tweet_id, datetime_created_on, username, rendered_content, conversation_id, like_count, reply_count, retweet_count, quote_count, source_label, source_url, url, user_all_data, tweet_all_data, media_url, media_filename, media_duration, media_views, media_content_blob, quoted_tweet_id, retweeted_tweet_id, replied_to_tweet_id)
)
    conn.commit()    
    return

def insert_into_junction_table(table_name, original_tweet_id, new_tweet_id, username, datetime_created_on):
    if "quoted" in table_name:
        table_name = QUOTED_TWEETS_JUNCTION_TABLE_NAME
        new_id_name = JUNCTION_TALBES[QUOTED_TWEETS_JUNCTION_TABLE_NAME]
    elif "replied" in table_name:
        table_name = REPLIED_TO_TWEETS_JUNCTION_TABLE_NAME
        new_id_name = JUNCTION_TALBES[REPLIED_TO_TWEETS_JUNCTION_TABLE_NAME]
    elif "retweets" in table_name:
        table_name = RETWEETS_TO_TWEETS_JUNCTION_TABLE_NAME
        new_id_name = JUNCTION_TALBES[RETWEETS_TO_TWEETS_JUNCTION_TABLE_NAME]
    else:
        error_message = "Junction table '" + table_name + "' does not exist - aborting attempt"
        # logging.error(error_message)
        return
    elapsed_time, tweets_saved_per_second = get_stats()
    print_inserting_into_db_message(table_name, new_tweet_id, username, datetime_created_on)
    c.execute("INSERT INTO %s (original_tweet_id, %s) VALUES (?, ?)" %(table_name, new_id_name), (original_tweet_id, new_tweet_id))
    conn.commit()
    return

def get_tweet(original_tweet_id=None, new_tweet_id=None, table_name=None, tweet=None):
    # this utilizes recursion to archive the entire chain of retweets, quoted tweets and replied to tweets
    # logging.info("Pulling data for tweet with ID " + str(new_tweet_id))
    tweet_counter()

    if tweet is None:
        try:
            _tmp_tweet = enumerate(sntwitter.TwitterTweetScraper(str(new_tweet_id)).get_items())
            for _tmp,tweet in _tmp_tweet:
                tweet = tweet
        except Exception as e: 
            tweet_cannot_be_retrieved_message = "Tweet could not be retrieved. It's most likely been deleted."
            # logging.error(tweet_cannot_be_retrieved_message)
            print(tweet_cannot_be_retrieved_message)
            insert_into_main_table(table_name, new_tweet_id, None, None, tweet_cannot_be_retrieved_message, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
            if table_name is not ORIGINAL_ACCOUNT_TWEETS:
                insert_into_junction_table(table_name, original_tweet_id, new_tweet_id)
            return

    # placing these two vars first for later parts to function
    tweet_id = tweet.id 
    username = tweet.user.username


    datetime_created_on = str(tweet.date)
    conversation_id = tweet.conversationId
    like_count = tweet.likeCount
    media = tweet.media
    media_duration = None
    media_views = None
    media_url = None
    media_filename = None
    media_content_blob = None
    media_content = None
    if media is not None:
        media_type = str(type(media[0]))
        media_content = media[0]
        if "Video" in media_type:
            media_duration = media_content.duration
            media_views = media_content.views
            media_url = (media_content.variants)[0].url #Twitter can, but does not always, save more than one version type ("variant" in snscrape) per video/gif. We'll use the first variant, because (1) this will keep the DB simpler and (2) after testing, the first variant appears to always be the highest-quality version
            media_filename = str(username) + "_" + str(tweet_id) + ".mp4"
            media_content = download_media(media_url, media_filename) 
        elif "Photo" in media_type:
            media_url = media_content.fullUrl
            media_filename = str(username) + "_" + str(tweet_id) + ".jpg"
            media_content = download_media(media_url, media_filename)
        elif "Gif" in media_type:
            media_url = (media_content.variants)[0].url
            media_filename = str(username) + "_" + str(tweet_id) + ".mp4"
            media_content = download_media(media_url, media_filename)
        
        if media_content is None:
            media_content_blob = None
        else:
            media_content_blob = convert_to_binary_data(media_content[0])
            os.remove(media_filename) #a file's not created if the media resource no longer exists
    
    quote_count = tweet.quoteCount
    quoted_tweet = tweet.quotedTweet
    if quoted_tweet is not None:
        quoted_tweet_id = (tweet.quotedTweet).id
        get_tweet(tweet_id, quoted_tweet_id, QUOTED_TWEETS_TABLE_NAME)
    else:
        quoted_tweet_id = None
    rendered_content = tweet.renderedContent
    replied_to_tweet_id = tweet.inReplyToTweetId
    if replied_to_tweet_id is not None:
        get_tweet(tweet_id, replied_to_tweet_id, REPLIED_TO_TWEETS_TABLE_NAME)
    reply_count = tweet.replyCount
    retweet_count = tweet.retweetCount
    retweeted_tweet = tweet.retweetedTweet
    if retweeted_tweet is not None:
        retweeted_tweet_id = (tweet.retweetedTweet).id
        get_tweet(tweet_id, retweeted_tweet_id, RETWEETS_TABLE_NAME)        
    else:
        retweeted_tweet_id = None
    source_label = tweet.sourceLabel
    source_url = tweet.sourceUrl
    
    url = tweet.url
    user_all_data = tweet.user.json()
    tweet_all_data = tweet.json()
    
    insert_into_main_table(table_name, tweet_id, datetime_created_on, username, rendered_content, conversation_id, like_count, reply_count, retweet_count, quote_count, source_label, source_url, url, user_all_data, tweet_all_data, media_url, media_filename, media_duration, media_views, media_content_blob, quoted_tweet_id, retweeted_tweet_id, replied_to_tweet_id)
    if table_name is not ORIGINAL_ACCOUNT_TWEETS:
        insert_into_junction_table(table_name, original_tweet_id, new_tweet_id, username, datetime_created_on)

def main():
    create_tweet_counter()
    create_exception_counter()
    initialize_databases()
    for _tmp,tweet in enumerate(sntwitter.TwitterSearchScraper('''from:%s include:nativeretweets''' %TWITTER_ACCOUNT).get_items()):
        get_tweet(None, None, ORIGINAL_ACCOUNT_TWEETS, tweet)

# -------------------------------
# --- END OF SCRIPT FUNCTIONS ---
# -------------------------------

main()
