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
db_name = (cwd + "/archives/twitter_archive.db")+'?check_same_thread=False'  # noqa
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
    links = Column('links', String)
    mentioned_users = Column('mentioned_users', String)
    place_country = Column('place_country', String)
    place_country_code = Column('place_country_code', String)
    place_full_name = Column('place_full_name', String)
    place_name = Column('place_name', String)
    place_type = Column('place_type', String)
    quote_count = Column('quote_count', BigInteger)
    reply_count = Column('reply_count', BigInteger)
    recount = Column('recount', Integer)
    replied_to_id = Column('replied_to_id', Integer, ForeignKey('tweets.id'))
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
    description_links = Column('description_links', String)
    display_name = Column('display_name', String)
    favorites_count = Column('favorites_count', Integer)
    followers_count = Column('followers_count', Integer)
    friends_count = Column('friends_count', Integer)
    label = Column('label', String)
    links = Column('links', String)
    listed_count = Column('listed_count', Integer)
    location = Column('location', String)
    protected_account = Column('protected_account', String)
    status_count = Column('status_count', Integer)
    url = Column('url', String)
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


class WebPagesTable(Base):
    __tablename__ = "web_pages"
    id = Column(String, primary_key=True)
    url = Column("url", String)
    warc = Column("warc", BLOB)
    html = Column("html", String)
    plaintext = Column("plaintext", String)
    pdf = Column("pdf", BLOB)
    internet_archive_link = Column("internet_archive_link", String)
    archive_today_link = Column("archive_today_link", String)


class WebpagesTweetsTable(Base):
    __tablename__ = "webpages_tweets"
    webpage_id = Column("webpage_id",
                        ForeignKey("web_pages.id"),
                        primary_key=True
                        )
    tweet_id = Column("tweet_id", ForeignKey("tweets.id"), primary_key=True)


class WebpagesUsersTable(Base):
    __tablename__ = "webpages_users"
    webpage_id = Column("webpage_id",
                        ForeignKey("web_pages.id"),
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


tweet_exists_counter = CounterExists()
user_exists_counter = CounterExists()
media_exists_counter = CounterExists()
tweet_counter = Counter()
user_counter = Counter()
media_counter = Counter()
webpage_counter = Counter()
webpage_exists_counter = Counter()


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
        dt = dt.strftime("%Y.%m.%d %H:%M:%S")

    return dt


class ProgramStats:
    def __init__(self,
                 tweet=None,
                 user=None,
                 media_id=None,
                 ):
        self.tweet = tweet
        self.user = user
        self.id = None
        self.dt = None
        self.username = None
        self.obj_type = None
        self.conversation_id = None
        if tweet is not None:
            self.username = tweet.user.username
            self.dt = get_datetime(dt=tweet.date, string_conversion=True)
            self.id = tweet.id
            self.obj_type = "Tweet"
            self.conversation_id = tweet.conversationId
        elif user is not None:
            self.username = user.username
            self.dt = get_datetime(dt=user.created, string_conversion=True)
            self.id = user.id
            self.obj_type = "User"
        elif media_id is not None:
            self.id = str(media_id)[:15]
            self.obj_type = "Media"

        self.tweets_saved = tweet_counter.value()
        self.users_saved = user_counter.value()
        self.medias_saved = media_counter.value()
        self.webpage_saved = webpage_counter.value()

        self.tweets_skipped = tweet_exists_counter.value()
        self.users_skipped = user_exists_counter.value()
        self.medias_skipped = media_exists_counter.value()
        self.webpage_skipped = webpage_exists_counter.value()

        self.tweets_total = self.tweets_saved + self.tweets_skipped
        self.users_total = self.users_saved + self.users_skipped
        self.medias_total = self.medias_saved + self.medias_skipped
        self.webpage_total = self.webpage_saved + self.webpage_skipped

        self.total_skipped = self.tweets_skipped + self.users_skipped + self.medias_skipped + self.webpage_skipped  # noqa
        self.total_saved = self.tweets_saved + self.users_saved + self.medias_saved + self.webpage_skipped  # noqa
        self.total_total = self.tweets_total + self.users_total + self.medias_total + self.webpage_total  # noqa

        elapsed_time = datetime.now() - start_time
        et_float = elapsed_time.total_seconds()
        self.elapsed_time = elapsed_time
        self.elapsed_time = datetime.now() - start_time

        self.tweet_saves_sec = round((self.tweets_saved/ et_float), 1)  # noqa
        self.tweet_skips_sec = round((self.tweets_skipped / et_float), 1)
        self.tweet_ops_sec = round(((self.tweets_total) / et_float), 1)

        self.user_saves_sec = round((self.users_saved / et_float), 1)
        self.user_skips_sec = round((self.users_skipped / et_float), 1)
        self.user_ops_sec = round(((self.user_saves_sec + self.user_skips_sec) / et_float), 1)  # noqa

        self.media_saves_sec = round((self.medias_saved / et_float), 1)
        self.media_skips_sec = round((self.medias_skipped / et_float), 1)
        self.media_ops_sec = round(((self.medias_total) / et_float), 1)

        self.webpage_saves_sec = round((self.webpage_saved / et_float), 1)
        self.webpage_skips_sec = round((self.webpage_skipped / et_float), 1)
        self.webpage_ops_sec = round(((self.webpage_total) / et_float), 1)

        self.total_saves_sec = round((self.total_saved / et_float), 1)
        self.total_skips_sec = round((self.total_skipped / et_float), 1)
        self.total_ops_sec = round(((self.total_total) / et_float), 1)

    def print_stats(self):
        table = [
                ["Current Time", get_datetime(datetime.now(), string_conversion=True), "", ""],  # noqa
                ["Elapsed Time", self.elapsed_time, "", ""],
                ["", "", "", ""],
                ["Saving", self.obj_type, "", ""],
                ["Username", f"@{self.username}", "", ""],
                ["Datetime", self.dt, "", ""],
                ["ID", self.id, "", ""],
                ["Conversation ID", self.conversation_id, "", ""],
                ["", "", "", ""],
                ["", "", "", ""],
                ["", "Saved", "Skipped", "Total"],
                ["Saves", "", "", ""],
                ["  Tweets",
                 "{:,}".format(self.tweets_saved),
                 "{:,}".format(self.tweets_skipped),
                 "{:,}".format(self.tweets_total),
                 ],
                ["  Users",
                 "{:,}".format(self.users_saved),
                 "{:,}".format(self.users_skipped),
                 "{:,}".format(self.users_total),
                 ],
                ["  Media",
                 "{:,}".format(self.medias_saved),
                 "{:,}".format(self.medias_skipped),
                 "{:,}".format(self.medias_total),
                 ],
                 ["  Webpages",
                  "{:,}".format(self.webpage_saved),
                  "{:,}".format(self.webpage_skipped),
                  "{:,}".format(self.webpage_total),
                  ],
                ["Total",
                 "{:,}".format(self.total_saved),
                 "{:,}".format(self.total_skipped),
                 "{:,}".format(self.total_total),
                 ],
                ["", "", "", ""],
                ["Ops/sec", "", "", ""],
                ["  Tweets",
                 "{:,}".format(self.tweet_saves_sec),
                 "{:,}".format(self.tweet_skips_sec),
                 "{:,}".format(self.tweet_ops_sec),
                 ],
                ["  Users",
                 "{:,}".format(self.user_saves_sec),
                 "{:,}".format(self.user_skips_sec),
                 "{:,}".format(self.user_ops_sec),
                 ],
                ["  Media",
                 "{:,}".format(self.media_saves_sec),
                 "{:,}".format(self.media_skips_sec),
                 "{:,}".format(self.media_ops_sec),
                 ],
                 ["  Webpages",
                  "{:,}".format(self.webpage_saves_sec),
                  "{:,}".format(self.webpage_skips_sec),
                  "{:,}".format(self.media_ops_sec),
                  ],
                ["Total",
                 "{:,}".format(self.total_saves_sec),
                 "{:,}".format(self.total_skips_sec),
                 "{:,}".format(self.total_ops_sec),
                 ],
                ]
        headers = ["Value", "Stats", "", ""]  # noqa
        tabulate.PRESERVE_WHITESPACE = True
        print(tabulate.tabulate(table, headers, tablefmt="presto", numalign="left", stralign="left",))  # noqa
        print("\n")


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
    fn = cwd + "/m3u8/" + str(id) + random_seed + ".mp4"
    content_blob = None
    try:
        subprocess.run(['ffmpeg', '-i', url, '-bsf:a', 'aac_adtstoasc', '-vcodec', 'copy', '-c', 'copy', '-crf', '50', fn], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)  # noqa
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

    if url is not None:
        exists = thread_session.query(MediaTable).filter(MediaTable.url == url)  # noqa
        exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        if exists is True:
            media_exists_counter.increment()
            thread_session.close()
            return
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
                logger.error(e)
                pass
    else:
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

    if content_blob is not None:
        id = sha512(content_blob).hexdigest()
        exists = thread_session.query(MediaTable).filter(MediaTable.id == id)  # noqa
        exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        if exists is True:
            media_exists_counter.increment()
            try:
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
                pass
        else:
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
            except Exception as e:  # noqa
                # logger.error(e)
                pass
        try:
            thread_session.commit()
            # logger.debug(f"Saved Media ID: {id}")
        except Exception as e:
            if "UNIQUE constraint" not in str(e):
                logger.error(e)
                media_exists_counter.increment()
                thread_session.close()
                return id
        media_counter.increment()
    thread_session.close()
    ProgramStats(media_id=id).print_stats()
    return id


def save_webpage(url, twitter_id, type):
    thread_session = db_session()
    webpage_id = sha512(str(url).encode('utf-8')).hexdigest()

    def check_exists(webpage_id, twitter_id, table):
        exists = False
        if table == WebPagesTable:
            exists = thread_session.query(WebPagesTable).filter(WebPagesTable.id == str(webpage_id))  # noqa
            exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        elif table == WebpagesTweetsTable:
            exists_tweet = thread_session.query(WebpagesTweetsTable).filter(WebpagesTweetsTable.tweet_id == str(twitter_id))  # noqa
            exists_tweet = thread_session.query(literal(True)).filter(exists_tweet.exists()).scalar()  # noqa
            exists_webpage = thread_session.query(WebpagesTweetsTable).filter(WebpagesTweetsTable.webpage_id == str(webpage_id))  # noqa
            exists_webpage = thread_session.query(literal(True)).filter(exists_webpage.exists()).scalar()  # noqa
            if exists_tweet is True and exists_webpage is True:
                exists = True
            else:
                exists = False
        elif table == WebpagesUsersTable:
            exists_user = thread_session.query(WebpagesUsersTable).filter(WebpagesUsersTable.user_id == str(twitter_id))  # noqa
            exists_user = thread_session.query(literal(True)).filter(exists_user.exists()).scalar()  # noqa
            exists_webpage = thread_session.query(WebpagesUsersTable).filter(WebpagesUsersTable.webpage_id == str(webpage_id))  # noqa
            exists_webpage = thread_session.query(literal(True)).filter(exists_webpage.exists()).scalar()  # noqa
            if exists_user is True and exists_webpage is True:
                exists = True
            else:
                exists = False
        else:
            exists = False
        if exists is None:
            exists = False
        return exists

    if type == TweetTable:
        table = WebpagesTweetsTable
    elif type == UserTable:
        table = WebpagesUsersTable

    if check_exists(webpage_id, twitter_id, table) is True:
        webpage_exists_counter.increment()
        return  # TODO increment stats
    else:
        try:
            if table == WebpagesTweetsTable:
                thread_session.add_all([table(
                            webpage_id=webpage_id,
                            tweet_id=twitter_id,
                        )])
            elif table == WebpagesUsersTable:
                thread_session.add_all([table(
                            webpage_id=webpage_id,
                            user_id=twitter_id,
                        )])
            if check_exists(webpage_id, None, WebPagesTable) is True:
                webpage_exists_counter.increment()
            else:
                # TODO: download archive
                # if it does not exists upload to archive
                warc = None
                html = None
                plaintext = None
                pdf = None
                internet_archive_link = None
                archive_today_link = None
                thread_session.add_all([WebPagesTable(
                                id=webpage_id,
                                url=url,
                                warc=warc,
                                html=html,
                                plaintext=plaintext,
                                pdf=pdf,
                                internet_archive_link=internet_archive_link,
                                archive_today_link=archive_today_link,
                            )])
                webpage_counter.increment()
            thread_session.commit()
        except Exception as e:  # noqa
            if "UNIQUE constraint" not in str(e):
                # logger.error(e)
                pass
        thread_session.close()


def save_user(user):
    """Saves a user/twitter account profile

    Args:
        user (snscrape.Tweet.User): User object
    """
    thread_session = db_session()

    lbl = None
    if user.label is not None:
        lbl = user.label.description

    description_links = None
    dl = user.descriptionLinks
    if dl is not None:
        description_links = (dl[0]).url
        save_webpage(description_links, user.id, UserTable)

    links = None
    ul = user.link
    if ul is not None:
        links = ul.url
        save_webpage(links, user.id, UserTable)

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
            description=user.renderedDescription,
            description_links=description_links,
            display_name=user.displayname,
            favorites_count=user.favouritesCount,
            followers_count=user.followersCount,
            friends_count=user.friendsCount,
            label=lbl,
            links=links,
            listed_count=user.listedCount,
            location=user.location,
            protected_account=user.protected,
            status_count=user.statusesCount,
            url=user.url,
            username=user.username,
            verified=user.verified,
        )])
    except Exception as e:  # noqa
        logger.error(e)
        thread_session.close()
        return

    try:
        thread_session.commit()
    except Exception as e:
        if "UNIQUE constraint" not in str(e):
            # logger.error(e)
            user_exists_counter.increment()
            thread_session.close()
            return
    # logger.debug(f"Saved Username: {user.username}")
    user_counter.increment()
    thread_session.close()
    ProgramStats(user=user).print_stats()


def get_tweet_by_id(new_tweet_id: int):
    """Archives a tweet given a tweet's ID

    Args:
        new_tweet_id (int): New tweet to be archived (e.g., a
        reply tweet)
    """
    thread_session = db_session()
    exists = thread_session.query(TweetTable).filter(TweetTable.id == new_tweet_id)  # noqa
    exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
    if exists is True:
        tweet_exists_counter.increment()
        thread_session.close()
        return

    try:
        tmp_tweet = enumerate(sntwitter.TwitterTweetScraper(str(
                                new_tweet_id)).get_items())
    except Exception:
        # logger.debug(f'''Tweet could not be retrieved. It's most likely been deleted. Tweet ID: {new_tweet_id}''')  # noqa
        thread_session.close()
        return

    for _, single_tweet in tmp_tweet:
        thread_session.close()
        return single_tweet


def save_tweet(tweet):
    """Saves a tweet and its metadata. If applicable,
    links tweets together.

    Args:
        tweet (snscrape.Tweet): Tweet object
    """
    if type(tweet) is sntwitter.TweetRef:
        tweet = get_tweet_by_id(tweet.id)

    thread_session = db_session()

    def check_exists(term, table):
        exists = False
        if table is TweetTable:
            exists = thread_session.query(TweetTable).filter(TweetTable.id == str(term))  # noqa
            exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        elif table is UserTable:
            exists = thread_session.query(UserTable).filter(UserTable.username == str(term))  # noqa
            exists = thread_session.query(literal(True)).filter(exists.exists()).scalar()  # noqa
        if exists is None:
            exists = False
        return exists

    if check_exists(tweet.id, TweetTable) is True:
        tweet_exists_counter.increment()
        thread_session.close()
        return

    if check_exists(tweet.user.username, UserTable) is True:
        user_exists_counter.increment()
    else:
        try:
            save_user(tweet.user)
        except Exception as e:  # noqa
            pass
            # logger.error(e)
            pass

    conversation_id = tweet.conversationId

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

    links = None
    tl = tweet.links
    if tl is not None:
        links = (tl[0]).url
        save_webpage(links, tweet.id, TweetTable)
    if tweet.media is not None:
        for media in tweet.media:
            save_media(media, tweet.id, None, None)
    users_mentioned = tweet.mentionedUsers
    if tweet.mentionedUsers is not None:
        _users_mentioned = ""
        for user in tweet.mentionedUsers:
            if check_exists(user.username, UserTable) is True:
                user_exists_counter.increment()
            else:
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

    replied_to_id = tweet.inReplyToTweetId

    try:
        thread_session.add_all([TweetTable(
            id=tweet.id,
            content=tweet.rawContent,
            creation_datetime=get_datetime(dt=tweet.date),
            conversation_id=conversation_id,
            hashtags=hashtags,
            language=tweet.lang,
            latitude=lat,
            longitude=lon,
            like_count=tweet.likeCount,
            links=links,
            mentioned_users=users_mentioned,
            place_country=pl_country,
            place_country_code=pl_country_code,
            place_full_name=pl_full_name,
            place_name=pl_name,
            place_type=pl_type,
            quote_count=tweet.quoteCount,
            recount=tweet.retweetCount,
            replied_to_id=replied_to_id,
            reply_count=tweet.replyCount,
            source_app=tweet.sourceLabel,
            url=tweet.url,
            user_id=tweet.user.id,
            username=tweet.user.username,
            vibe=tweet.vibe,
            view_count=tweet.viewCount,
        )])
    except Exception as e:  # noqa
        # logger.error(e)
        pass

    try:
        thread_session.commit()
        tweet_counter.increment()
    except Exception as e:
        if "UNIQUE constraint" not in str(e):
            # logger.error(e)
            tweet_exists_counter.increment()
    thread_session.close()

    ProgramStats(tweet=tweet).print_stats()

    if tweet.quotedTweet is not None:
        if check_exists(tweet.quotedTweet.id, TweetTable) is True:
            tweet_exists_counter.increment()
        else:
            save_tweet(tweet.quotedTweet)
    if tweet.retweetedTweet is not None:
        if check_exists(tweet.retweetedTweet.id, TweetTable) is True:
            tweet_exists_counter.increment()
        else:
            save_tweet(tweet.retweetedTweet)
    if replied_to_id is not None:
        rp_tweet = get_tweet_by_id(replied_to_id)
        if rp_tweet is not None:
            if check_exists(rp_tweet.id, TweetTable) is True:
                tweet_exists_counter.increment()
            else:
                save_tweet(rp_tweet)

    if conversation_id is not None:
        for _tmp, c_tweet in enumerate(sntwitter.TwitterSearchScraper(f'''
                conversation_id:{conversation_id}
                -filter:unsafe (filter:safe OR -filter:safe)"
                ''').get_items()):
            if c_tweet is not None:
                if check_exists(c_tweet.id, TweetTable) is True:
                    tweet_exists_counter.increment()
                else:
                    save_tweet(c_tweet)


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
        # save_tweet(tweet)


def main():
    # logger.debug("Initializing database")
    Base.metadata.create_all(engine, checkfirst=True)
    # ln = len(TWITTER_ACCOUNTS)
    for chunk in grouper(TWITTER_ACCOUNTS, 12):
        with ThreadPoolExecutor() as executor:
            for account in chunk:
                executor.submit(archive_accounts, account)
    # for account in TWITTER_ACCOUNTS:
    #     archive_accounts(account)

    db_session.close()
    logger.info("Finished program")


if __name__ == '__main__':
    main()
