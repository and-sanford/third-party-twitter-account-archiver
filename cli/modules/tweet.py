#!/usr/bin/env python3
# custom libs
import modules.database as db
from modules.media import ArchivedMedia
from modules.user import ArchivedUser
import modules.settings as settings

# third-party libs
import snscrape.modules.twitter as sntwitter

logger = settings.logger


def archive_tweet_by_id(new_tweet_id: int, referrer_tweet_id: int):
    """Archives a tweet given the tweet's ID, rather than
    by a search (which is how the rest of the program
    functions).

    Args:
        new_tweet_id (int): New tweet to be archived (e.g., a
        reply tweet)
        referrer_tweet_id (int): The original tweet (e.g., the original
        tweet that was replied to).
    """
    logger.debug(f'''Archiving tweet {new_tweet_id} from referrer tweet {referrer_tweet_id}''') # noqa
    try:
        tmp_tweet = enumerate(sntwitter.TwitterTweetScraper(str(
                                new_tweet_id)).get_items())
    except Exception:
        logger.error('''Tweet could not be retrieved.
                     It's most likely been deleted.
                     '''
                     )
        return
    for _tmp, single_tweet in tmp_tweet:
        ArchivedTweet(single_tweet, referrer_tweet_id)


class ArchivedTweet(object):
    def __init__(self, tweet, referrer_tweet_id=None):
        logger.debug(f'''Creating ArchivedTweet object for tweet {tweet.id}''')
        self.id = tweet.id
        self.conversation_id = tweet.conversationId
        self.content = tweet.content
        self.creation_datetime = settings.get_datetime(dt=tweet.date)
        self.hashtags = None
        if tweet.hashtags is not None:
            _hashtags = ""
            for hashtag in tweet.hashtags:
                _hashtags += str(hashtag) + ", "
            self.hashtags = _hashtags.rstrip(', ')
        self.language = tweet.lang
        self.latitude = None
        self.longitude = None
        if tweet.coordinates is not None:
            self.latitude = tweet.coordinates.latitude
            self.longitude = tweet.coordinates.longitude
        self.like_count = tweet.likeCount
        if tweet.media is not None:
            for media in tweet.media:
                ArchivedMedia(
                            media=media,
                            save_name=(
                                        "tweet_id"
                                        ),
                            referrer_obj=self,
                            url=None,
                            )
        self.mentioned_users = None
        if tweet.mentionedUsers is not None:
            _mentioned_users = ""
            for user in tweet.mentionedUsers:
                ArchivedUser(user)
                _mentioned_users += str(user.username) + ", "
            self.mentioned_users = _mentioned_users.rstrip(', ')
        self.place_country = None
        self.place_country_code = None
        self.place_full_name = None
        self.place_name = None
        self.place_type = None
        if tweet.place is not None:
            self.place_country = tweet.place.country
            self.place_country_code = tweet.place.countryCode
            self.place_full_name = tweet.place.fullName
            self.place_name = tweet.place.name
            self.place_type = tweet.place.type
        self.quote_count = tweet.quoteCount
        self.quoted_id = None
        if tweet.quotedTweet is not None:
            self.quoted_id = (tweet.quotedTweet).id
            archive_tweet_by_id(
                                new_tweet_id=self.quoted_id,
                                referrer_tweet_id=self.id
                               )
        self.recount = tweet.retweetCount
        self.referrer_tweet_id = referrer_tweet_id
        self.replied_to_id = None
        if tweet.inReplyToTweetId is not None:
            self.replied_to_id = tweet.inReplyToTweetId
        self.reply_count = tweet.replyCount
        self.retweeted_id = None
        if tweet.retweetedTweet is not None:
            self.retweeted_id = (tweet.retweetedTweet).id
            archive_tweet_by_id(
                                new_tweet_id=self.retweeted_id,
                                referrer_tweet_id=self.id
                               )
        self.source_app = tweet.sourceLabel
        self.url = tweet.url
        self.user_id = tweet.user.id
        self.username = tweet.user.username
        ArchivedUser(tweet.user)
        db.insert_into_table(db.TweetTable.__tablename__, self, self.id)
