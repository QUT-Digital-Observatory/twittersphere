"""
This module is responsible for taking and storing Twitter related entities as
extracted in process.py into an SQLite database.

"""

import concurrent.futures as cf
import sqlite3
import time

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

        transaction_start = time.monotonic()
        db_conn.execute("begin")

        for page in pages:
            futures.add(pool.submit(process.process_page, page))

            if len(futures) > n_cpus + 1:
                done, futures = cf.wait(futures, return_when="FIRST_COMPLETED")

                for f in done:
                    to_insert = f.result()
                    insert_processed(db_conn, to_insert)

                # Commit every 10 seconds
                if time.monotonic() - transaction_start >= 10:
                    db_conn.execute("commit")
                    transaction_start = time.monotonic()
                    db_conn.execute("begin")

        db_conn.execute("commit")


def insert_processed(db_conn, processed):
    """
    Insert processed data into the database.

    A savepoint is wrapped around this transaction to ensure that partial
    writes aren't seen.

    """
    data = processed["data"]
    includes = processed["includes"]
    metadata = processed["metadata"]

    try:
        db_conn.execute("savepoint insert_page")
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
        users = data.get("users", []) + includes["users"]
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
            (data.get("users", [])),
        )

        # Process Tweets
        tweets = data.get("tweets", []) + includes["tweets"]
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
                :withheld_country_codes,
                :poll_id,
                :place_id
            )
            """,
            (tweet | metadata for tweet in tweets),
        )

        # Tweet ancillary - mentions, media, urls, hashtags, annotations
        db_conn.executemany(
            "insert or ignore into tweet_media values (?, ?, ?)",
            (
                (tweet["id"], metadata["retrieved_at"], media_key)
                for tweet in tweets
                for media_key in tweet["media_keys"]
            ),
        )

        db_conn.executemany(
            "insert or ignore into tweet_hashtag values (?, ?, ?)",
            (
                (tweet["id"], metadata["retrieved_at"], hashtag)
                for tweet in tweets
                for hashtag in tweet["hashtags"]
            ),
        )

        db_conn.executemany(
            "insert or ignore into tweet_cashtag values (?, ?, ?)",
            (
                (tweet["id"], metadata["retrieved_at"], cashtag)
                for tweet in tweets
                for cashtag in tweet["cashtags"]
            ),
        )

        db_conn.executemany(
            "insert or ignore into tweet_mention values (?, ?, ?, ?)",
            (
                (
                    tweet["id"],
                    metadata["retrieved_at"],
                    mention["user_id"],
                    mention["username"],
                )
                for tweet in tweets
                for mention in tweet["mentions"]
            ),
        )

        db_conn.executemany(
            """
            insert or ignore into url values (
                :url,
                :retrieved_at,
                :description,
                :display_url,
                :expanded_url,
                :images,
                :media_key,
                :status,
                :title,
                :unwound_url
            )
            """,
            (url | metadata for tweet in tweets for url in tweet["urls"]),
        )

        db_conn.executemany(
            """
            insert or ignore into tweet_url values (
                :id,
                :retrieved_at,
                :url
            )
            """,
            (
                (url["url"], metadata["retrieved_at"], tweet["id"])
                for tweet in tweets
                for url in tweet["urls"]
            ),
        )

        db_conn.executemany(
            "insert or ignore into directly_collected_tweet values(:id)",
            (data.get("tweets", [])),
        )

        db_conn.executemany(
            "insert or ignore into tweet_entity_domain values (?, ?, ?, ?)",
            (
                (
                    tweet["id"],
                    metadata["retrieved_at"],
                    annotation["entity_id"],
                    annotation["domain_id"],
                )
                for tweet in tweets
                for annotation in tweet["context_annotations"]
            ),
        )

        db_conn.executemany(
            "insert or ignore into entity values (:entity_id, :entity_name, :entity_description)",
            (
                annotation
                for tweet in tweets
                for annotation in tweet["context_annotations"]
            ),
        )

        db_conn.executemany(
            "insert or ignore into domain values (:domain_id, :domain_name, :domain_description)",
            (
                annotation
                for tweet in tweets
                for annotation in tweet["context_annotations"]
            ),
        )

        # Process polls
        db_conn.executemany(
            """
            insert or ignore into poll values (
                :id,
                :retrieved_at,
                :duration_minutes,
                :end_datetime,
                :voting_status
            )
            """,
            (poll | metadata for poll in includes["polls"]),
        )

        db_conn.executemany(
            """
            insert or ignore into poll_option values (
                :id,
                :retrieved_at,
                :position,
                :label,
                :votes
            )
            """,
            (
                option | poll | metadata
                for poll in includes["polls"]
                for option in poll["options"]
            ),
        )

        # Process Places
        db_conn.executemany(
            """
            insert or ignore into place values (
                :id,
                :country,
                :country_code,
                :full_name,
                :geo_type,
                :geo_bbox_1,
                :geo_bbox_2,
                :geo_bbox_3,
                :geo_bbox_4,
                :name,
                :place_type
            )
            """,
            includes["places"],
        )

        # Process media
        db_conn.executemany(
            """
            insert or ignore into media values (
                :media_key,
                :retrieved_at,
                :alt_text,
                :duration_ms,
                :preview_image_url,
                :view_count,
                :type,
                :url,
                :width,
                :height
            )
            """,
            (media | metadata for media in includes["media"]),
        )

    except Exception:
        db_conn.execute("rollback to insert_page")
        raise

    finally:
        db_conn.execute("release insert_page")
