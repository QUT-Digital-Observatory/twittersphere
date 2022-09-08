"""
This module is responsible for taking and storing Twitter related entities as
extracted in process.py into an SQLite database.

"""

import concurrent.futures as cf
import sqlite3

from . import process, _schema


class SchemaVersionError(Exception):
    pass


def ensure_db(db_path):
    """
    Return an SQLite connection with appropriate schema at the given path.

    If the schema version ondisk is out of date an error will be raised -
    currently there is no option to migrate forward, data must be
    reinserted.

    """
    conn = sqlite3.connect(db_path, isolation_level=None)

    try:
        schema_version = list(
            conn.execute(
                """
            SELECT value
            from metadata
            where key = 'twittersphere_schema_version'
            """
            )
        )[0][0]
    except sqlite3.OperationalError:
        schema_version = 0

    if schema_version == 0:
        # Execute migrations
        conn.execute("pragma journal_mode=WAL")
        conn.execute("begin")
        for statement in _schema.SCHEMA_STATEMENTS.split(";\n\n"):
            conn.execute(statement)

        conn.execute("commit")

    elif schema_version != _schema.CURRENT_SCHEMA_VERSION:
        raise SchemaVersionError(
            f"The on disk database has schema version {schema_version}, the "
            f"current version is {_schema.CURRENT_SCHEMA_VERSION}. You will "
            "need to create a new database and reinsert your data to use this "
            "version of twittersphere."
        )

    return conn


def insert_pages(db_conn, pages, n_cpus=None):
    """
    Process the stream of pages and insert them into the database.

    """
    futures = set()

    with cf.ProcessPoolExecutor(n_cpus) as pool:
        for page in pages:
            futures.add(pool.submit(process.process_page, page))

            if len(futures) > n_cpus + 1:
                done, futures = cf.wait(futures, return_when="FIRST_COMPLETED")

                try:
                    db_conn.execute("begin")

                    for f in done:
                        to_insert = f.result()
                        insert_processed(db_conn, to_insert)

                    db_conn.execute("commit")

                except:
                    db_conn.execute("rollback")


def insert_processed(db_conn, processed):
    """
    Insert processed data into the database.

    """
    main = processed["data"]
    includes = processed["includes"]
    metadata = processed["metadata"]

    # First, create the collection context id/metadata
    db_conn.execute(
        """
        insert or ignore into collection_context(
            retrieved_at,
            twitter_url,
            twarc_version
        ) values(:retrieved_at, :twitter_url, :twarc_version)
        """,
        metadata,
    )

    context_id = list(
        db_conn.execute(
            """
            select context_id
            from collection_context
            where (retrieved_at, twitter_url, twarc_version) =
                (:retrieved_at, :twitter_url, :twarc_version)
            """,
            metadata,
        )
    )[0][0]
    metadata["context_id"] = context_id

    # Process users
    users = main.get("users", []) + includes.get("users", [])
    db_conn.executemany(
        """
        insert or ignore into user_at_time values (
            :id,
            :context_id,
            :retrieved_at,
            :name,
            :profile_image_url,
            :created_at,
            :protected,
            :description,
            :location,
            :pinned_tweet_id,
            :verified,
            :url,
            :username,
            :followers_count,
            :following_count,
            :tweet_count,
            :listed_count,
            :withheld_country_codes
        )
        """,
        (user | metadata for user in users),
    )

    db_conn.executemany(
        "insert or ignore into directly_collected_user values(:id)",
        (main.get("users", [])),
    )

    # Process Tweets
    tweets = main.get("tweets", []) + includes.get("tweets", [])
    db_conn.executemany(
        """
        insert or ignore into tweet_at_time values (
            :id,
            :context_id,
            :author_id,
            :created_at,
            :retrieved_at,
            :conversation_id,
            :retweeted,
            :quoted,
            :replied,
            :text,
            :lang,
            :source,
            :possibly_sensitive,
            :reply_settings,
            :like_count,
            :quote_count,
            :reply_count,
            :retweet_count,
            :withheld_copyright,
            :withheld_country_codes
        )
        """,
        (tweet | metadata for tweet in tweets),
    )

    db_conn.executemany(
        "insert or ignore into directly_collected_tweet values(:id)",
        (main.get("tweets", [])),
    )
