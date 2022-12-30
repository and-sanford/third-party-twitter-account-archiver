#!/usr/bin/env python3
# custom libs
import modules.settings as settings

# third-party libs
from sqlalchemy import (
                        create_engine,
                        BLOB,
                        Column,
                        DateTime,
                        Float,
                        ForeignKey,
                        Integer,
                        MetaData,
                        String,
                        )
from sqlalchemy.orm import (
                            declarative_base,
                            Session
                            )

logger = settings.logger
database_name = settings.get_db_name()
engine = create_engine(f"sqlite:///{database_name}",
                       echo=False,
                       future=True
                       )
meta = MetaData()
Base = declarative_base()
session = Session(engine)


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
    like_count = Column('like_count', Integer)
    mentioned_users = Column('mentioned_users', String)
    place_country = Column('place_country', String)
    place_country_code = Column('place_country_code', String)
    place_full_name = Column('place_full_name', String)
    place_name = Column('place_name', String)
    place_type = Column('place_type', String)
    quote_count = Column('quote_count', Integer)
    quoted_id = Column('quoted_id', Integer, ForeignKey('tweets.id'))
    reply_count = Column('reply_count', Integer)
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


class UserTable(Base):
    __tablename__ = "users"
    id = Column('id', Integer, primary_key=True, unique=True)
    account_url = Column('account_url', String)
    banner_picture_hash = Column(
                            'banner_picture_hash',
                            Integer,
                            ForeignKey('media.id')
                            )
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
    media_count = Column('media_count', Integer)
    profile_picture_hash = Column(
                                'profile_picture_hash',
                                Integer,
                                ForeignKey('media.id')
                                )
    protected_account = Column('protected_account', String)
    status_count = Column('status_count', Integer)
    username = Column('username', String)
    verified = Column('verified', String)


class MediaTable(Base):
    __tablename__ = "media"
    id = Column('id', String, primary_key=True, unique=True)
    content_blob = Column('content_blob', BLOB)
    duration = Column('duration', Float)
    filename = Column('filename', String)
    url = Column('url', String)
    views = Column('views', Integer)


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


def get_table_obj(table_name=None, linked_table_name=None):
    """Gets the class of the custom Table object.

    Args:
        table_name (str, optional): Table name. Defaults to None.
        linked_table_name (str, optional): Name of junction table.
        Defaults to None.

    Returns:
        custom Table class: Empty object of custom Table class
    """
    logger.debug(f"Getting table obj for {table_name} | {linked_table_name}")
    if table_name is not None:
        if table_name is TweetTable.__tablename__:
            table = TweetTable
        elif table_name is UserTable.__tablename__:
            table = UserTable
        elif table_name is MediaTable.__tablename__:
            table = MediaTable
        elif table_name is MediaTweetsTable.__tablename__:
            table = MediaTweetsTable
        elif table_name is MediaUsersTable.__tablename__:
            table = MediaUsersTable
        else:
            logger.critical(f"Invalid table_name: {table_name}")
            exit()
    elif linked_table_name is not None:
        if linked_table_name is TweetTable.__tablename__:
            table = MediaTweetsTable
        elif linked_table_name is UserTable.__tablename__:
            table = MediaUsersTable
        else:
            logger.critical(f"Invalid table_name: {table_name}")
            exit()
    else:
        logger.critical(f"Invalid table_name: {table_name}")
        exit()

    return table


def check_if_row_exists(table,
                        row_pk=None,
                        media_id=None,
                        tweet_or_user_id=None
                        ):
    """Checks if a row already exists.

    Args:
        table (custom Table class): Table to be checked
        row_pk (int, optional): Primary key of the row to be
        checked. Defaults to None.
        media_id (string, optional): Hash value of the media.
        PK in the junction table. Defaults to None.
        tweet_or_user_id (int, optional): ID of tweet or user
        to be inserted into the junction table. Defaults to None.

    Returns:
        bool: If the row exists or not
    """
    logger.debug(f"Checking if row exists in {table} table")
    exists = False
    found = None
    with session.no_autoflush:
        if table is TweetTable or table is UserTable or table is MediaTable:
            found = session.query(table.id).filter_by(
                                            id=row_pk
                                            ).first()
        elif table is MediaTweetsTable:
            found = session.query(table).filter(
                                            table.media_id == media_id and
                                            table.tweet_id == tweet_or_user_id
                                            ).first()
        elif table is MediaUsersTable:
            found = session.query(table).filter(
                                            table.media_id == media_id and
                                            table.user_id == tweet_or_user_id
                                            ).first()
        else:
            logger.error(f"Couldn't identify table {table}")
    if found is not None:
        exists = True
        logger.debug(f"Row already exists in {table} table")
        settings.skipped_archive_counter()
    return exists


def insert_into_table(table_name,
                      insert_obj,
                      row_pk,
                      linked_table_name=None,
                      tweet_or_user_id=None):
    """Junction tables are checked separately - while
    a given piece of Media may already exist in the DB,
    it may need to be mapped to an additional Tweet or User.

    Args:
        table (Base): Custom Table class
        insert_obj (obj): Custom object - Tweet, User or Media
        row_pk (int): Primary key of the insert_obj. Not using insert_obj.id
        bc "Media" uses hash. So, in this case, I've opted to have the PK
        passed into the function.
        referrer_table_class (Class, optional): Custom junction Table class.
        Defaults to None.
        insert_junction_obj (_type_, optional): Custom object - Tweet or User.
        Defaults to None.
    """
    logger.debug(f"Inserting object into {table_name} table")
    table = get_table_obj(table_name=table_name)
    obj_already_exists = check_if_row_exists(table, row_pk=row_pk)
    if obj_already_exists is False:
        settings.archive_counter()
        if table is TweetTable:
            session.add_all([TweetTable(
                id=insert_obj.id,
                content=insert_obj.content,
                creation_datetime=insert_obj.creation_datetime,
                conversation_id=insert_obj.conversation_id,
                hashtags=insert_obj.hashtags,
                language=insert_obj.language,
                latitude=insert_obj.latitude,
                longitude=insert_obj.longitude,
                like_count=insert_obj.like_count,
                mentioned_users=insert_obj.mentioned_users,
                place_country=insert_obj.place_country,
                place_country_code=insert_obj.place_country_code,
                place_full_name=insert_obj.place_full_name,
                place_name=insert_obj.place_name,
                place_type=insert_obj.place_type,
                quote_count=insert_obj.quote_count,
                quoted_id=insert_obj.quoted_id,
                reply_count=insert_obj.reply_count,
                recount=insert_obj.recount,
                replied_to_id=insert_obj.replied_to_id,
                retweeted_id=insert_obj.retweeted_id,
                source_app=insert_obj.source_app,
                url=insert_obj.url,
                user_id=insert_obj.user_id,
                username=insert_obj.username,
            )])
        elif table is UserTable:
            session.add_all([UserTable(
                id=insert_obj.id,
                account_url=insert_obj.account_url,
                banner_picture_hash=insert_obj.banner_picture_hash,
                creation_datetime=insert_obj.creation_datetime,
                description=insert_obj.description,
                display_name=insert_obj.display_name,
                favorites_count=insert_obj.favorites_count,
                followers_count=insert_obj.followers_count,
                friends_count=insert_obj.friends_count,
                label=insert_obj.label,
                linked_url=insert_obj.linked_url,
                listed_count=insert_obj.listed_count,
                location=insert_obj.location,
                media_count=insert_obj.media_count,
                profile_picture_hash=insert_obj.profile_picture_hash,
                protected_account=insert_obj.protected_account,
                status_count=insert_obj.status_count,
                username=insert_obj.username,
                verified=insert_obj.verified,
            )])
        elif table is MediaTable:
            session.add_all([MediaTable(
                id=insert_obj.id,
                content_blob=insert_obj.content_blob,
                duration=insert_obj.duration,
                filename=insert_obj.filename,
                url=insert_obj.url,
                views=insert_obj.views,
            )])

            if (
               linked_table_name is not None and
               tweet_or_user_id is not None
               ):
                junction_table = get_table_obj(
                                        linked_table_name=linked_table_name
                                        )
                junction_obj_already_exists = check_if_row_exists(
                                              table=junction_table,
                                              media_id=insert_obj.id,
                                              tweet_or_user_id=tweet_or_user_id
                                              )

                if junction_obj_already_exists is False:
                    logger.debug(f'''Adding {tweet_or_user_id} and {insert_obj.id} to the {junction_table} table''')  # noqa
                    if junction_table is MediaTweetsTable:
                        session.add_all([
                            MediaTweetsTable(
                                media_id=insert_obj.id,
                                tweet_id=tweet_or_user_id,
                            )])
                    elif junction_table is MediaUsersTable:
                        session.add_all([
                            MediaUsersTable(
                                media_id=insert_obj.id,
                                user_id=tweet_or_user_id,
                            )])
        session.commit()

    settings.print_stats(table_name, insert_obj)


logger.debug("Initializing database")
Base.metadata.create_all(engine, checkfirst=True)
