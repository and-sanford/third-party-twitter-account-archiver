#!/usr/bin/env python3
# custom libs
from modules.tweet import ArchivedTweet
import modules.settings as settings

# third-party libs
import snscrape.modules.twitter as sntwitter


def main():
    """Archives tweets, user data and media from
    the user's chosen twitter accounts
    """
    settings.init()
    logger = settings.logger
    logger.info(f"Starting to archive the following accounts {settings.TWITTER_ACCOUNTS}")  # noqa
    for account in settings.TWITTER_ACCOUNTS:
        for _tmp, tweet in enumerate(sntwitter.TwitterSearchScraper(f'''
                                    from:{account}
                                    include:nativeretweets
                                    ''',
                                     top=True).get_items()):
            settings.PARENT_ACCOUNT = account
            settings.PARENT_TWEET = tweet.id
            settings.PARENT_TIME = settings.get_datetime(dt=tweet.date)
            ArchivedTweet(tweet)


if __name__ == "__main__":
    main()
