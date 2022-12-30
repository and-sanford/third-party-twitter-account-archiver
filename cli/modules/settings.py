#!/usr/bin/env python3
# native libs
from datetime import datetime, timezone
import os

# third-party libs
import tabulate
import structlog
import logging
import logging.config


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


def init():
    """Initializes global variables
    """
    logger.debug("Creating global variables")

    global ARCHIVED_ITEMS_COUNT
    global DATABASE_NAME
    global LOG_FILENAME
    global SKIPPED_ITEMS_COUNT
    global START_TIME
    global TOTAL_ITEMS_ARCHIVED
    global TWITTER_ACCOUNTS

    ARCHIVED_ITEMS_COUNT = 0
    SKIPPED_ITEMS_COUNT = 0
    START_TIME = get_datetime()
    TOTAL_ITEMS_ARCHIVED = 0
    TWITTER_ACCOUNTS = ["example1", "example2"]


LOG_FILENAME = "cli/logs/" + get_datetime(save_file=True) + ".log"  # noqa
DATABASE_NAME = "cli/archives/twitter_archive_" + get_datetime(save_file=True) + ".db"  # noqa


def archive_counter():
    """
    Counts the number items (i.e., tweets, users and media)
    that have been archived.

    Due to this program's use of recursive, at times the
    ARCHIVED_ITEMS_COUNT value printed on-screen may seem
    inaccurate; it's counting correctly. Once the recursive
    function exits its "rabbit hole," the correct amount will
    be printed
    """
    global ARCHIVED_ITEMS_COUNT
    ARCHIVED_ITEMS_COUNT += 1


def skipped_archive_counter():
    """Create global vars for skipped items
    """
    global SKIPPED_ITEMS_COUNT
    SKIPPED_ITEMS_COUNT += 1


def total_items_archived_counter():
    """Create global var for total items archived
    """
    global TOTAL_ITEMS_ARCHIVED
    TOTAL_ITEMS_ARCHIVED = ARCHIVED_ITEMS_COUNT + SKIPPED_ITEMS_COUNT


def get_db_name() -> str:
    """Returns the database name.
    For some reason, just calling
    the DATABASE_NAME var did not
    work.

    Returns:
        str: Name of database
    """
    return DATABASE_NAME


def get_stats():
    """Gets stats related to the program running,
    such as elapsed time, saves/sec, etc.

    Returns:
        datetime, float, str: Different stats that
        are formatted in different ways
    """
    elapsed_time = get_datetime() - START_TIME
    elapsed_time_mins = elapsed_time.total_seconds()
    saves_per_second = round((ARCHIVED_ITEMS_COUNT/elapsed_time_mins), 1)
    attempted_saves_per_second = round(
                                        (TOTAL_ITEMS_ARCHIVED /
                                         elapsed_time_mins),
                                        1
                                      )
    total_items_archived_counter()
    return str(elapsed_time)[:-4], saves_per_second, attempted_saves_per_second


def print_stats(table_name, saved_obj):
    """Provides the end-user with real-time
    status updates in the console

    Args:
        table_name (str): Name of current table that
        data's being inputted into
        saved_obj (custom): A custom ArchivedTweet or
        Archived User object,from which the data's being
        saved (e.g., if a Tweet's being saved to the
        database, the saved_obj will be an ArchivedTweet)
    """
    elapsed_time, saves_per_second, attempted_saves_per_second = get_stats()
    now = get_datetime(string_conversion=True)
    if table_name == "tweets" or table_name == "users":
        username = saved_obj.username
        creation_datetime = saved_obj.creation_datetime
        if creation_datetime is None:
            creation_datetime = "n/a"
        else:
            creation_datetime = get_datetime(dt=creation_datetime,
                                             string_conversion=True
                                             )
    else:
        username = "n/a"
        creation_datetime = "n/a"

    col_width = 19
    saved_obj_id = str(saved_obj.id)[:col_width]

    db_size = str(f"{round(os.path.getsize(DATABASE_NAME)/1024/1024, 1)} MB")

    table = [
        ["Current Time", now],
        ["Elapsed Time", elapsed_time],
        ["Table", table_name],
        ["ID (truncated)", saved_obj_id],
        ["User", str(f"@{username}")],
        ["Creation Date", creation_datetime],
        ["", ""],
        ["Saves/sec", saves_per_second],
        ["Attempted saves/sec", attempted_saves_per_second],
        ["Archived", ARCHIVED_ITEMS_COUNT],
        ["Previously archived", SKIPPED_ITEMS_COUNT],
        ["Total Items Archived", TOTAL_ITEMS_ARCHIVED],
        ["DB Size", db_size]
    ]
    headers = ["Stat", "Value"]
    tabulate.PRESERVE_WHITESPACE = True
    print(tabulate.tabulate(table, headers, tablefmt="pretty", numalign="left", stralign="left", maxcolwidths=30))  # noqa
    print("\n")


timestamper = structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S")
pre_chain = [
    structlog.stdlib.add_log_level,
    structlog.stdlib.ExtraAdder(),
    timestamper,
]


def extract_from_record(_, __, event_dict):
    """
    Extract thread and process names and add them
    to the event dict.
    """
    record = event_dict["_record"]
    event_dict["process_name"] = record.processName
    event_dict["thread_name"] = record.threadName

    return event_dict


logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": [
                    structlog.stdlib.filter_by_level,
                    structlog.stdlib.add_logger_name,
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.PositionalArgumentsFormatter(),
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.UnicodeDecoder(),
                    structlog.processors.CallsiteParameterAdder(
                        {
                            structlog.processors.CallsiteParameter.FILENAME,
                            structlog.processors.CallsiteParameter.FUNC_NAME,
                            structlog.processors.CallsiteParameter.LINENO,
                        }
                    ),
                    structlog.processors.JSONRenderer()
                ],
                "foreign_pre_chain": pre_chain,
            },
            "colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processors": [
                   extract_from_record,
                   structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                   structlog.dev.ConsoleRenderer(colors=True),
                ],
                "foreign_pre_chain": pre_chain,
            },
        },
        "handlers": {
            "default": {
                "level": "ERROR",
                "class": "logging.StreamHandler",
                "formatter": "colored",
            },
            "file": {
                "level": "DEBUG",
                "class": "logging.handlers.WatchedFileHandler",
                "filename": LOG_FILENAME,
                "formatter": "plain",
            },
        },
        "loggers": {
            __name__: {
                "handlers": ["default", "file"],
                "level": "DEBUG",
                "propagate": False,
            },
        }
})
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

'''
for some reason, an initial log here
is required for logger to actually log
events to the console and file throughout
the entire program
'''
logger.info("Starting logger")
