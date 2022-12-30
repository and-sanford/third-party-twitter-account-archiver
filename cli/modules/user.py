#!/usr/bin/env python3
# module libs
import modules.database as db
from modules.media import ArchivedMedia

# third-party libs
import modules.settings as settings

logger = settings.logger


class ArchivedUser(object):
    def __init__(self, user):
        logger.debug(f'''Creating ArchivedUser object for user {user.id}''')
        self.id = user.id
        self.account_url = user.url
        self.banner_picture_hash = ArchivedMedia(
                                media=None,
                                save_name=(
                                    user.username +
                                    "_banner_picture"
                                    ),
                                referrer_obj=self,
                                url=user.profileImageUrl,
                               ).id
        self.creation_datetime = settings.get_datetime(dt=user.created)
        self.description = user.description
        self.display_name = user.displayname
        self.favorites_count = user.favouritesCount
        self.followers_count = user.followersCount
        self.friends_count = user.friendsCount
        self.label = user.label
        if self.label is not None:
            self.label = user.label.description
        self.linked_url = user.linkUrl
        self.listed_count = user.listedCount
        self.location = user.location
        self.media_count = user.mediaCount
        self.profile_picture_hash = ArchivedMedia(
                                media=None,
                                save_name=(
                                    user.username +
                                    "_profile_picture"
                                    ),
                                referrer_obj=self,
                                url=user.profileImageUrl,
                               ).id
        self.protected_account = user.protected
        self.status_count = user.statusesCount
        self.username = user.username
        self.verified = user.verified
        db.insert_into_table(db.UserTable.__tablename__, self, self.id)
