#!/usr/bin/env python3
import itertools
import os
import random
import subprocess
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha512
from itertools import zip_longest

import snscrape.modules.twitter as sntwitter
import tabulate
from loguru import logger
from sqlalchemy import (BLOB, BigInteger, Column, DateTime, Float, ForeignKey,
                        Integer, MetaData, String, create_engine, literal)
from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool

lock = threading.Lock()
start_time = datetime.now()

meta = MetaData()
Base = declarative_base()
cwd = os.getcwd()
db_name = (cwd + "/threading/archives/twitter_archive_media.db")+'?check_same_thread=False'  # noqa
engine = create_engine(f"sqlite:///{db_name}",
                       echo=False,
                       future=True,
                       poolclass=QueuePool
                       )
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))


def grouper(iterable, n, fillvalue=None):
    """Enables global counts
    """
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


class TweetTable(Base):
    __tablename__ = "tweets"
    id = Column('id', Integer, primary_key=True, unique=True)
    content = Column('content', String)
    creation_datetime = Column('creation_datetime', DateTime)
    conversation_id = Column('conversation_id', Integer)
    hashtags = Column('hashtags', String)
    language = Column('language', String)
    latitude = Column('latitude', Float)
    longitude = Column('longitude', Float)
    like_count = Column('like_count', BigInteger)
    mentioned_users = Column('mentioned_users', String)
    place_country = Column('place_country', String)
    place_country_code = Column('place_country_code', String)
    place_full_name = Column('place_full_name', String)
    place_name = Column('place_name', String)
    place_type = Column('place_type', String)
    quote_count = Column('quote_count', BigInteger)
    quoted_id = Column('quoted_id', Integer, ForeignKey('tweets.id'))
    reply_count = Column('reply_count', BigInteger)
    recount = Column('recount', Integer)
    replied_to_id = Column('replied_to_id', Integer, ForeignKey('tweets.id'))
    referrer_tweet_id = Column('referrer_tweet_id',
                               Integer,
                               ForeignKey('tweets.id')
                               )
    retweeted_id = Column('retweeted_id', Integer, ForeignKey('tweets.id'))
    source_app = Column('source_app', String)
    url = Column('url', String)
    user_id = Column("user_id", Integer, ForeignKey('users.id'))
    username = Column('username', String)
    vibe = Column('vibe', String)
    view_count = Column("view_count", BigInteger)


class UserTable(Base):
    __tablename__ = "users"
    id = Column('id', Integer, primary_key=True, unique=True)
    account_url = Column('account_url', String)
    creation_datetime = Column('creation_datetime', DateTime)
    description = Column('description', String)
    display_name = Column('display_name', String)
    favorites_count = Column('favorites_count', Integer)
    followers_count = Column('followers_count', Integer)
    friends_count = Column('friends_count', Integer)
    label = Column('label', String)
    linked_url = Column('linked_url', String)
    listed_count = Column('listed_count', Integer)
    location = Column('location', String)
    protected_account = Column('protected_account', String)
    status_count = Column('status_count', Integer)
    username = Column('username', String)
    verified = Column('verified', String)


class MediaTable(Base):
    __tablename__ = "media"
    id = Column('id', String, primary_key=True, unique=True)
    content_blob = Column('content_blob', BLOB)
    alt_text = Column('alt_text', String)
    duration = Column('duration', Float)
    url = Column('url', String)
    views = Column('views', Integer)
    thumbnail_id = Column('thumbnail_id', String, ForeignKey('media.id'))


class MediaTweetsTable(Base):
    __tablename__ = "media_tweets"
    media_id = Column("media_id",
                      ForeignKey("media.id"),
                      primary_key=True
                      )
    tweet_id = Column("tweet_id", ForeignKey("tweets.id"), primary_key=True)


class MediaUsersTable(Base):
    __tablename__ = "media_users"
    media_id = Column("media_id",
                      ForeignKey("media.id"),
                      primary_key=True
                      )
    user_id = Column("user_id", ForeignKey("users.id"), primary_key=True)


TWITTER_ACCOUNTS = ["example1", "example2"]


class Counter:
    def __init__(self):
        self._incs = itertools.count()
        self._accesses = itertools.count()

    def increment(self):
        next(self._incs)

    def value(self):
        return next(self._incs) - next(self._accesses)


class CounterExists:
    def __init__(self):
        self._incs = itertools.count()
        self._accesses = itertools.count()

    def increment(self):
        next(self._incs)

    def value(self):
        return next(self._incs) - next(self._accesses)


global_exists_counter = CounterExists()
my_global_counter = Counter()


def get_datetime(dt=None, string_conversion=False, save_file=False):
    """Standardizes datetime by converting all datetime
    values to UTC. If no datetime object is inputted,
    the current datetime is returned.

    Args:
        dt (datetime, optional): Convert an existing
        dt object. Defaults to None.
        string_conversion (bool, optional): Convert
        datetime to string. Defaults to False.

    Returns:
        datetime, str: Depending on the options chosen, will return
        either a datetime or string object
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    else:
        if dt.tzinfo != timezone.utc:
            dt.replace(tzinfo=timezone.utc).timestamp()

    if save_file is True:
        dt = dt.strftime("%Y%m%d.%H%M%S-utc")
    elif string_conversion is True:
        dt = dt.strftime("%Y.%m.%d %H.%M.%S UTC")

    return dt


def convert_m3u8(url, id):
    """Converts m3u8 video URLs to
    mp4. Twitter recently started
    encoding at least some of their
    videos in m3u8 playlist format.

    Args:
        url (string): m3u8 playlist url
        id (int): Tweet or User ID

    Returns:
        content_blob (BLOB): Binary version of mp4 file
        fn (string): Filename, to be deleted later
    """
    random.seed(id)
    r = random.randint(0, id)
    n = datetime.now().strftime("%M%S%f")
    random_seed = str(r) + str(n)
    fn = cwd + "/threading/m3u8/" + str(id) + random_seed + ".mp4"
    content_blob = None
    try:
        subprocess.run(['ffmpeg', '-i', url, '-bsf:a', 'aac_adtstoasc', '-vcodec', 'copy', '-c', 'copy', '-crf', '50', fn], stdout=subprocess.DEVNULL)  # noqa
    except Exception as e:
        logger.error(e)
        return None
    try:
        open(fn, 'rb')
    except Exception as e:
        logger.error(e)
        return None
    with open(fn, 'rb') as file:
        content_blob = file.read()
    return content_blob, fn



def save_media(media, tweet_or_user_id: int, username: str, url: str):  # noqa
    """Saves media objects. Assigns each
    a unique ID (which is a sha256 hash)
    to avoid duplicates.

    Args:
        media (snscrape.Tweet.Media): Media object
        tweet_or_user_id (int): Tweet or User ID
        username (str): Username
        url (str): Media object's URL

    Returns:
        int: Media object ID
    """
    thread_session = db_session()
    # logger.debug(f"Getting media from tweet or user id {tweet_or_user_id}")
    content_blob = None
    id = None
    duration = None
    views = None
    alt_text = None
    thumbnail_id = None

    if url is None:
        '''For gifs/videos, Twitter can, but does not always,
        save the file in more than one format and/or quality
        level. ("variant" in snscrape).
        We'll use the first variant after testing, the first variant
        appears to always be the highest-quality version'''
        if media is not None:
            media_type = str(type(media))
            alt_text = media.altText
            if "Video" in media_type:
                duration = media.duration
                url = (media.variants)[0].url
                views = media.views
                if media.thumbnailUrl is not None:
                    thumbnail_id = save_media(None,
                                              tweet_or_user_id,
                                              None,
                                              media.thumbnailUrl)
            elif "Photo" in media_type:
                url = media.fullUrl
            elif "Gif" in media_type:
                url = (media.variants)[0].url
    # logger.debug(f"Downloading media at {url}")
    if ".m3u8" in url:
        content_blob, fn = convert_m3u8(url, tweet_or_user_id)
        try:
            os.remove(fn)
        except Exception as e:
            logger.error(e)
    else:
        try:
            content_blob = (urllib.request.urlopen(url)).read()
        except Exception as e:  # noqa
            # logger.error(e)
            pass
    if content_blob is not None:
        id = sha512(content_blob).hexdigest()
        try:
            thread_session.add_all([MediaTable(
                    id=id,
                    content_blob=content_blob,
                    alt_text=alt_text,
                    duration=duration,
                    url=url,
                    views=views,
                    thumbnail_id=thumbnail_id,
                )])
            if username is None:
                thread_session.add_all([MediaTweetsTable(
                        media_id=id,
                        tweet_id=tweet_or_user_id,
                    )])
            else:
                thread_session.add_all([MediaUsersTable(
                        media_id=id,
                        user_id=tweet_or_user_id,
                    )])
        except Exception as e:  # noqa
            # logger.error(e)
            thread_session.close()
            return id

        try:
            thread_session.commit()
            # logger.debug(f"Saved Media ID: {id}")
        except Exception as e:
            if "UNIQUE constraint" not in str(e):
                # logger.error(e)
                global_exists_counter.increment()
                thread_session.close()
                return id
        my_global_counter.increment()
    thread_session.close()
    return id


def save_user(user):
    """Saves a user/twitter account profile

    Args:
        user (snscrape.Tweet.User): User object
    """
    thread_session = db_session()
    exists = thread_session.query(UserTable).filter(UserTable.id == user.id)  # noqa
    exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
    if exists is True:
        global_exists_counter.increment()
        thread_session.close()
        return

    lbl = None
    if user.label is not None:
        lbl = user.label.description

    url = user.profileImageUrl
    if url is not None:
        save_media(None, user.id, user.username, url)
        pass

    if user.profileBannerUrl is not None:
        save_media(None, user.id, user.username, url)
        pass

    try:
        thread_session.add_all([UserTable(
            id=user.id,
            account_url=user.url,
            creation_datetime=get_datetime(dt=user.created),
            description=user.description,
            display_name=user.displayname,
            favorites_count=user.favouritesCount,
            followers_count=user.followersCount,
            friends_count=user.friendsCount,
            label=lbl,
            linked_url=user.linkUrl,
            listed_count=user.listedCount,
            location=user.location,
            protected_account=user.protected,
            status_count=user.statusesCount,
            username=user.username,
            verified=user.verified,
        )])
    except Exception as e:  # noqa
        # logger.error(e)
        thread_session.close()
        return

    try:
        thread_session.commit()
    except Exception as e:
        if "UNIQUE constraint" not in e:
            # logger.error(e)
            global_exists_counter.increment()
            thread_session.close()
            return
    # logger.debug(f"Saved Username: {user.username}")
    my_global_counter.increment()
    thread_session.close()


def get_tweet_by_id(new_tweet_id: int, referrer_tweet_id: int):
    """Archives a tweet given a tweet's ID

    Args:
        new_tweet_id (int): New tweet to be archived (e.g., a
        reply tweet)
        referrer_tweet_id (int): The original tweet (e.g., the original
        tweet that was replied to).
    """
    try:
        tmp_tweet = enumerate(sntwitter.TwitterTweetScraper(str(
                                new_tweet_id)).get_items())
    except Exception:
        # logger.debug(f'''Tweet could not be retrieved. It's most likely been deleted. Tweet ID: {new_tweet_id}''')  # noqa
        return

    for _, single_tweet in tmp_tweet:
        return single_tweet


def save_tweet(tweet, referrer_tweet_id=None):
    """Saves a tweet and its metadata. If applicable,
    links tweets together.

    Args:
        tweet (snscrape.Tweet): Tweet object
        referrer_tweet_id (int, optional): Foreign key of the originating
        tweet (e.g., the previous tweet in a thread). Defaults to None.
    """
    thread_session = db_session()
    exists = thread_session.query(TweetTable).filter(TweetTable.id == tweet.id)  # noqa
    exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
    if exists is True:
        global_exists_counter.increment()
        thread_session.close()
        return

    save_user(tweet.user)

    hashtags = None
    if hashtags is not None:
        _hashtags = ""
        for tag in hashtags:
            _hashtags += str(tag) + ", "
        hashtags = _hashtags.rstrip(', ')

    lat = None
    lon = None
    if tweet.coordinates is not None:
        lat = tweet.coordinates.latitude
        lon = tweet.coordinates.longitude

    if tweet.media is not None:
        for media in tweet.media:
            save_media(media, tweet.id, None, None)
    users_mentioned = tweet.mentionedUsers
    if tweet.mentionedUsers is not None:
        _users_mentioned = ""
        for user in tweet.mentionedUsers:
            save_user(user)
            _users_mentioned += str(user.username) + ", "
        users_mentioned = _users_mentioned.rstrip(', ')

    pl_country = None
    pl_country_code = None
    pl_full_name = None
    pl_name = None
    pl_type = None
    if tweet.place is not None:
        pl_country = tweet.place.country
        pl_country_code = tweet.place.countryCode
        pl_full_name = tweet.place.fullName
        pl_name = tweet.place.name
        pl_type = tweet.place.type

    q_tweet = None
    if tweet.quotedTweet is not None:
        q_tweet = (tweet.quotedTweet).id
        exists = thread_session.query(TweetTable).filter(TweetTable.id == q_tweet)  # noqa
        exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        if exists is False:
            new_tweet = get_tweet_by_id(q_tweet, tweet.id)
            save_tweet(new_tweet, q_tweet)

    r_tweet = None
    if tweet.retweetedTweet is not None:
        r_tweet = (tweet.retweetedTweet).id
        exists = thread_session.query(TweetTable).filter(TweetTable.id == r_tweet)  # noqa
        exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        if exists is False:
            new_tweet = get_tweet_by_id(r_tweet, tweet.id)
            save_tweet(new_tweet, r_tweet)

    try:
        thread_session.add_all([TweetTable(
            id=tweet.id,
            content=tweet.content,
            creation_datetime=get_datetime(dt=tweet.date),
            conversation_id=tweet.conversationId,
            hashtags=hashtags,
            language=tweet.lang,
            latitude=lat,
            longitude=lon,
            like_count=tweet.likeCount,
            mentioned_users=users_mentioned,
            place_country=pl_country,
            place_country_code=pl_country_code,
            place_full_name=pl_full_name,
            place_name=pl_name,
            place_type=pl_type,
            quote_count=tweet.quoteCount,
            quoted_id=q_tweet,
            recount=tweet.retweetCount,
            replied_to_id=tweet.inReplyToTweetId,
            reply_count=tweet.replyCount,
            referrer_tweet_id=referrer_tweet_id,
            retweeted_id=r_tweet,
            source_app=tweet.sourceLabel,
            url=tweet.url,
            user_id=tweet.user.id,
            username=tweet.user.username,
            vibe=tweet.vibe,
            view_count=tweet.viewCount,
        )])
    except Exception as e:  # noqa
        # logger.error(e)
        thread_session.close()
        return

    try:
        thread_session.commit()
    except Exception as e:
        if "UNIQUE constraint" not in e:
            # logger.error(e)
            global_exists_counter.increment()
            thread_session.close()
            return

    # logger.debug(f"Saved Tweet ID: {tweet.id}")
    my_global_counter.increment()
    thread_session.close()


def archive_accounts(account):
    """Archives the tweets of a given twitter
    user/account

    Args:
        account (str): Twitter user/account handle
    """
    for _tmp, tweet in enumerate(sntwitter.TwitterSearchScraper(f'''
                                    from:{account}
                                    include:nativeretweets
                                    ''').get_items()):
        with ThreadPoolExecutor() as ex:
            ex.submit(save_tweet, tweet)

        elapsed_time = datetime.now() - start_time
        et_float = elapsed_time.total_seconds()
        saves_sec = round((my_global_counter.value() / et_float), 1)
        skips_sec = round((global_exists_counter.value() / et_float), 1)
        ops_sec = round(((my_global_counter.value() + global_exists_counter.value()) / et_float), 1)  # noqa

        table = [
                ["Current Time", datetime.now()],  # noqa
                ["Elapsed Time", elapsed_time],
                ["", ""],
                ["Current Account", f"@{account}"],
                ["Archived Items", "{:,}".format(my_global_counter.value())],
                ["Skipped Items", "{:,}".format(global_exists_counter.value())],  # noqa
                ["", ""],
                ["Saves/sec", saves_sec],
                ["Skips/sec", skips_sec],
                ["Ops/sec", ops_sec],
                ]
        headers = ["Value", "Stats",]  # noqa
        tabulate.PRESERVE_WHITESPACE = True
        print(tabulate.tabulate(table, headers, tablefmt="pretty", numalign="left", stralign="left", maxcolwidths=30))  # noqa
        print("\n")


def main():
    """Let's gooooo!!
    """
    # logger.debug("Initializing database")
    Base.metadata.create_all(engine, checkfirst=True)
    ln = len(TWITTER_ACCOUNTS)
    for chunk in grouper(TWITTER_ACCOUNTS, ln):
        with ThreadPoolExecutor() as executor:
            for account in chunk:
                executor.submit(archive_accounts, account)

    db_session.close()
    elapsed_time = datetime.now() - start_time
    et_float = elapsed_time.total_seconds()
    saves_sec = round((my_global_counter.value() / et_float), 1)
    ops_sec = round(((my_global_counter.value() + global_exists_counter.value()) / et_float), 1)  # noqa

    logger.info(f"Finished program in {elapsed_time}\n\tItems Archived:\t{my_global_counter.value()}\nAvg saves/sec:\t{saves_sec}\nAvg ops/sec:\t{ops_sec}")  # noqa


if __name__ == '__main__':
    main()
