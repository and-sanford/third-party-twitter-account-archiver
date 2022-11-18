# importing libraries and packages
import datetime
import json
import snscrape.modules.twitter as sntwitter
import sqlite3 

START_TIME = datetime.datetime.now()
conn = sqlite3.connect('[REPLACE WITH USERNAME. e.g., 'jack' (not @ sign)]') 
c = conn.cursor()
c.execute('''
          DROP TABLE [REPLACE WITH USERNAME. e.g., 'jack' (not @ sign)]
          ''')
c.execute('''
            CREATE TABLE IF NOT EXISTS [REPLACE WITH USERNAME. e.g., 'jack' (not @ sign)]
            (
            [row_id] INTEGER PRIMARY KEY, 
            [tweet_id] INTEGER, 
            [datetime] TEXT,
            [username] TEXT,
            [rendered_content] TEXT,
            [content] TEXT,
            [media] TEXT,
            [in_reply_to_tweet_id] INTEGER,
            [retweeted_tweet] TEXT,
            [quoted_tweet] TEXT,
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
            [retweeted_tweet_id] INTEGER,
            [retweeted_to_tweet_datetime] TEXT,
            [retweeted_to_tweet_username] TEXT,
            [retweeted_to_tweet_content] TEXT,
            [retweeted_to_tweet_sourceLabel] TEXT,
            [retweeted_to_tweet_quotedTweet] TEXT,
            [retweeted_to_tweet_user_all_data] TEXT,
            [retweeted_to_tweet_all_data] TEXT,
            [replied_to_tweet_id] INTEGER,
            [replied_to_tweet_datetime] TEXT,
            [replied_to_tweet_username] TEXT,
            [replied_to_tweet_content] TEXT,
            [replied_to_tweet_sourceLabel] TEXT,
            [replied_to_tweet_quotedTweet] TEXT,
            [replied_to_tweet_user_all_data] TEXT,
            [replied_to_tweet_all_data] TEXT
            ) 
            ''')
conn.commit()

# Using snstwitter to scrape and save data
for search_count,tweet in enumerate(sntwitter.TwitterSearchScraper('from:[REPLACE WITH USERNAME. e.g., 'jack' (not @ sign)] include:nativeretweets').get_items()):
    retweeted_tweet_id = None
    retweeted_to_tweet_username = None
    retweeted_to_tweet_user_all_data = None
    retweeted_to_tweet_datetime = None
    retweeted_to_tweet_content = None
    retweeted_to_tweet_sourceLabel = None
    retweeted_to_tweet_quotedTweet = None
    retweeted_to_tweet_all_data = None

    replied_to_tweet_id = None
    replied_to_tweet_username = None
    replied_to_tweet_user_all_data = None
    replied_to_tweet_datetime = None
    replied_to_tweet_content = None
    replied_to_tweet_sourceLabel = None
    replied_to_tweet_quotedTweet = None
    replied_to_tweet_all_data = None

    datetime_created_on = str(tweet.date)
    content = tweet.content
    conversationID = tweet.conversationId
    tweet_id = tweet.id
    inReplyToTweetId = tweet.inReplyToTweetId
    like_count = tweet.likeCount
    media = tweet.media
    media_string = ''
    if media is not None:
        for item in media:
            media_string.join(str(item))
    quote_count = tweet.quoteCount
    quoted_tweet = tweet.quotedTweet
    if quoted_tweet is not None:
        quoted_tweet = tweet.quotedTweet.json()
    rendered_content = tweet.renderedContent
    reply_count = tweet.replyCount
    retweet_count = tweet.retweetCount
    retweetedTweet = tweet.retweetedTweet
    
    source_label = tweet.sourceLabel
    source_url = tweet.sourceUrl
    url = tweet.url
    username = tweet.user.username
    user_all_data = tweet.user.json()
    tweet_all_data = tweet.json()

    if retweetedTweet is not None:
        retweeted_tweet_id = retweetedTweet.id
        retweeted_to_tweet_username = retweetedTweet.user.username
        retweeted_to_tweet_user_all_data = retweetedTweet.user.json()
        retweeted_to_tweet_datetime = retweetedTweet.date
        retweeted_to_tweet_content = retweetedTweet.renderedContent
        retweeted_to_tweet_sourceLabel = retweetedTweet.sourceLabel
        retweeted_to_tweet_quotedTweet = retweetedTweet.quotedTweet
        if retweeted_to_tweet_quotedTweet is not None:
            retweeted_to_tweet_quotedTweet = retweetedTweet.quotedTweet.json()
        retweeted_to_tweet_all_data = retweetedTweet.json()
    retweetedTweet = retweeted_to_tweet_all_data

    if inReplyToTweetId is not None:
        try:
            enumerate(sntwitter.TwitterTweetScraper(str(inReplyToTweetId)).get_items())
            for j,replied_to_tweet in enumerate(sntwitter.TwitterTweetScraper(str(inReplyToTweetId)).get_items()):
                replied_to_tweet_id = inReplyToTweetId
                replied_to_tweet_username = replied_to_tweet.user.username
                replied_to_tweet_user_all_data = replied_to_tweet.user.json()
                replied_to_tweet_datetime = replied_to_tweet.date
                replied_to_tweet_content = replied_to_tweet.renderedContent
                replied_to_tweet_sourceLabel = replied_to_tweet.sourceLabel
                replied_to_tweet_quotedTweet = replied_to_tweet.quotedTweet
                if replied_to_tweet_quotedTweet is not None:
                    replied_to_tweet_quotedTweet = replied_to_tweet.quotedTweet.json()
                replied_to_tweet_all_data = replied_to_tweet.json()
        except Exception as e:
            print(e)
            replied_to_tweet_content = "Tweet cannot be retrieved. It's most likely been deleted."
            continue

    current_datetime = datetime.datetime.now()
    current_datetime_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    elapsed_time = current_datetime - START_TIME
    elapsed_time_mins = elapsed_time.total_seconds()
    tweets_saved_per_second = round((search_count/elapsed_time_mins), 1)
    print("\n>>> Inserting into DB:\nCurrent Time:\t", current_datetime_string, "\nElapsed Time:\t", elapsed_time, "\nSaves/sec:\t", tweets_saved_per_second, "\nCount:\t\t", search_count, "\nTweet ID:\t", tweet_id, "\nTweet User:\t", "@",username, "\nTweet Date:\t", datetime_created_on)

    c.execute("INSERT INTO [REPLACE WITH USERNAME. e.g., 'jack' (not @ sign)] (tweet_id, datetime, username, rendered_content, content, media, in_reply_to_tweet_id, retweeted_tweet, quoted_tweet, conversation_id, like_count, reply_count, retweet_count, quote_count, source_label, source_url, url, user_all_data, tweet_all_data, retweeted_tweet_id, retweeted_to_tweet_datetime, retweeted_to_tweet_username, retweeted_to_tweet_content, retweeted_to_tweet_sourceLabel, retweeted_to_tweet_quotedTweet, retweeted_to_tweet_user_all_data, retweeted_to_tweet_all_data, replied_to_tweet_id, replied_to_tweet_datetime, replied_to_tweet_username, replied_to_tweet_content, replied_to_tweet_sourceLabel, replied_to_tweet_quotedTweet, replied_to_tweet_user_all_data, replied_to_tweet_all_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (tweet_id, datetime_created_on, username, rendered_content, content, media_string, inReplyToTweetId, retweetedTweet, quoted_tweet, conversationID, like_count, reply_count, retweet_count, quote_count, source_label, source_url, url, user_all_data, tweet_all_data, retweeted_tweet_id, retweeted_to_tweet_datetime, retweeted_to_tweet_username, retweeted_to_tweet_content, retweeted_to_tweet_sourceLabel, retweeted_to_tweet_quotedTweet, retweeted_to_tweet_user_all_data, retweeted_to_tweet_all_data, replied_to_tweet_id, replied_to_tweet_datetime, replied_to_tweet_username,replied_to_tweet_content, replied_to_tweet_sourceLabel, replied_to_tweet_quotedTweet, replied_to_tweet_user_all_data, replied_to_tweet_all_data))
    conn.commit()
