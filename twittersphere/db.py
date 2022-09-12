"""
This module is responsible for taking and storing Twitter related entities as
extracted in process.py into an SQLite database.

"""

import concurrent.futures as cf
import sqlite3
import threading
import time

from . import process, _schema, twittersphere


class SchemaVersionError(Exception):
    pass


def ensure_db(db_path):
    """
    Return an SQLite connection with appropriate schema at the given path.

    If the schema version ondisk is out of date an error will be raised -
    currently there is no option to migrate forward, data must be
    reinserted.

    """
    conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)

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


def flush_db(db_conn, target_db_path):
    """
    Flush the contents of db_conn into target_db_path, using SQLite 'attach'
    and insert in sorted primary key order.

    This is used to optimise insert using staging to an in memory database and
    periodic flushing to disk.

    """

    db_conn.execute("attach ? as flush_to", (target_db_path,))
    db_conn.execute("begin")

    for table, primary_key in _schema.table_keys.items():
        db_conn.execute(
            f"""
            insert or ignore into flush_to.{table}
            select *
            from main.{table}
            order by {primary_key}
            """
        )
    db_conn.execute("commit")
    db_conn.close()


def insert_pages(db_path, raw_pages, n_cpus=2, in_memory_max_db_size=2**30):
    """
    Process the stream of pages and insert them into the database.

    Data is staged to an in memory database until it hits
    `in_memory_max_db_size`, then it is flushed to the target database. The
    default size is 1GiB. For larger collections, using more memory is likely
    to make the process go faster.

    """
    futures = set()

    # Ensure the path exists for inserting with the correct schema.
    check_db = ensure_db(db_path)
    check_db.close()

    db_conn = ensure_db(":memory:")

    target = None
    batch_size = 0
    batch = []

    # Dispatch batches of 1 megabyte of raw content at a time.
    min_batch_size = 2**20

    with cf.ProcessPoolExecutor(n_cpus) as pool:

        db_conn.execute("begin")

        for raw_page in raw_pages:

            batch.append(raw_page)
            batch_size += len(raw_page)

            if batch_size >= min_batch_size:
                futures.add(pool.submit(process.process_pages, batch))
                batch = []
                batch_size = 0

            if len(futures) > n_cpus + 1:
                done, futures = cf.wait(futures, return_when="FIRST_COMPLETED")

                for f in done:
                    results_to_insert = f.result()
                    for result in results_to_insert:
                        insert_processed(db_conn, result)

                # Commit periodically from the in memory database
                db_size = list(
                    db_conn.execute(
                        """
                        select
                            (select page_count from pragma_page_count) *
                            (select page_size from pragma_page_size)
                        """
                    )
                )[0][0]

                if db_size >= in_memory_max_db_size:
                    db_conn.execute("commit")
                    # Wait for the previous db flush to finish
                    if target is not None:
                        target.join()

                    flush_db_conn = db_conn
                    target = threading.Thread(
                        target=flush_db, args=(flush_db_conn, db_path)
                    )
                    target.start()

                    # Start a new in memory database
                    db_conn = ensure_db(":memory:")
                    db_conn.execute("begin")

        futures.add(pool.submit(process.process_pages, batch))

        for f in cf.as_completed(futures):
            results_to_insert = f.result()
            for result in results_to_insert:
                insert_processed(db_conn, result)

        db_conn.execute("commit")

        if target is not None:
            target.join()

        flush_db(db_conn, db_path)


def insert_processed(db_conn, processed):
    """
    Insert processed data into the database.

    A savepoint is wrapped around this transaction to ensure that partial
    writes aren't seen.

    """
    data = processed["data"]
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
        (annotation for tweet in tweets for annotation in tweet["context_annotations"]),
    )

    db_conn.executemany(
        "insert or ignore into domain values (:domain_id, :domain_name, :domain_description)",
        (annotation for tweet in tweets for annotation in tweet["context_annotations"]),
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
