#!/usr/bin/env python3
# custom libs
import modules.database as db

# native libs
from hashlib import sha512
import os
import urllib

# third-party libs
import modules.settings as settings

logger = settings.logger


class ArchivedMedia(object):
    def __init__(self,
                 media,
                 save_name: str,
                 referrer_obj: object,
                 url: str):
        logger.debug(f'''Creating ArchivedMedia object for media at {url}''')
        self.media = media
        self.save_name = save_name
        self.tweet_or_user_id = referrer_obj.id
        self.url = url,
        (self.content_blob,
         self.id,
         self.duration,
         self.filename,
         self.url,
         self.views,
         ) = self.get_media()

        if self.content_blob is not None:
            referrer_obj_class = str(type(referrer_obj))
            if "ArchivedTweet" in referrer_obj_class:
                linked_table_name = db.TweetTable.__tablename__
            elif "ArchivedUser" in referrer_obj_class:
                linked_table_name = db.UserTable.__tablename__
            db.insert_into_table(db.MediaTable.__tablename__,
                                 self,
                                 self.id,
                                 linked_table_name,
                                 referrer_obj.id
                                 )

    def download_media(self, url, filename):
        """Downloads media in the format it's been provided in

        Args:
            url (str): URL of media to be downloaded
            filename (str): Filename of the media

        Returns:
            snscrape obj: Downloaded media - can be
            various formats (mp4, jpeg, etc.)
        """
        logger.debug(f"Downloading media at {url}")
        try:
            # media is named username_tweet_id.[filetype]
            downloaded_media = urllib.request.urlretrieve(
                                                            url,
                                                            filename
                                                         )
        except Exception as e:
            ignore_exception = "expected string or bytes-like object"
            try:
                if ignore_exception not in e.args[0]:
                    logger.error(e)
                    return None
            except Exception as e2:
                logger.error(e2)

        return downloaded_media

    def convert_to_binary_data(filename: str):
        """Enables media to be saved to a databse
        by converting the media from its downloaded
        format (e.g., mp4, jpeg) to a BLoB (binary) file.

        Args:
            filename (str): Name of media to be converted

        Returns:
            BLoB: Binary version of media
        """
        logger.debug(f"Converting media, {filename}, to binary")
        try:
            open(filename, 'rb')
        except Exception as e:
            logger.error(e)
            return None
        with open(filename, 'rb') as file:
            blob_data = file.read()
        blob_data_hash = sha512(blob_data).hexdigest()
        return blob_data, blob_data_hash

    def get_media(self):
        """Retrieves database-savable version of media,
        along with (if available) various stats

        Args:
            media (snscrape): snscrape media object. Defaults to None.
            tweet_or_user_id (int): String version of a tweet's or user's ID.
            Defaults to None.
            username (str): String version of username. Defaults to None.
            url (str, optional): URL of media to download. Defaults to None.
            filename (str, optional): Name to save. Defaults to None.

        Returns:
            content_blob (binary): binary version of media
            duration (str): Duration of media. Defaults to None.
            url (str): URL of media. Defaults to None.
            views (str): Number of views for the media. Defaults to None.
            filename (str): Name of media. Defaults to None.
        """
        media = self.media
        tweet_or_user_id = self.tweet_or_user_id
        save_name = self.save_name
        logger.debug(f"Getting media from tweet or user id {tweet_or_user_id}")
        # blobs (as binary formats) are already quite compressed,
        # and attempts to compress create a number of complications
        content = None
        content_blob = None
        id = None
        duration = None
        url = self.url[0]
        views = None
        filename = self.save_name

        if url is not None:
            content = ArchivedMedia.download_media(self, url, filename)
        else:
            '''For gifs/videos, Twitter can, but does not always,
            save the file in more than one format and/or quality
            level. ("variant" in snscrape).
            We'll use the first variant after testing, the first variant
            appears to always be the highest-quality version'''
            if media is not None:
                media_type = str(type(media))
                if "Video" in media_type:
                    duration = media.duration
                    filename = (
                                str(save_name) +
                                "_" +
                                str(tweet_or_user_id) +
                                ".mp4"
                                )
                    url = (media.variants)[0].url
                    views = media.views
                elif "Photo" in media_type:
                    url = media.fullUrl
                    filename = (
                                        str(save_name) +
                                        "_" +
                                        str(tweet_or_user_id) +
                                        ".jpg"
                                    )
                elif "Gif" in media_type:
                    url = (media.variants)[0].url
                    filename = (
                                        str(save_name) +
                                        "_" +
                                        str(tweet_or_user_id) +
                                        ".mp4"
                                    )
                content = ArchivedMedia.download_media(self, url, filename)
        if content is not None:
            (content_blob,
             id) = ArchivedMedia.convert_to_binary_data(
                                                          content[0]
                                                         )
            if content_blob is not None:
                try:
                    os.remove(filename)
                except Exception as e:
                    logger.error(e)
                    pass
        return (
                content_blob,
                id,
                duration,
                filename,
                url,
                views
                )
