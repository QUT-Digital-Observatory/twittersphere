"""
The schema creation statements, and the associated index and materialised view
statements to be run after inserting data.

TODO:

- Likes, followers, following.

"""

CURRENT_SCHEMA_VERSION = 12

SCHEMA_STATEMENTS = """
CREATE table if not exists collection_context (
    context_id integer primary key,
    retrieved_at datetime,
    twitter_url text,
    twarc_version text,
    unique (retrieved_at, twitter_url, twarc_version)
);

CREATE table if not exists user_at_time (
/*
Design notes:
- This schema is an attempt to provide affordances for analytical work, while still
  representing as much as possible as we understand of the underlying data.
- All tables have defined primary keys and appropriate foreign key constraints
  that will be enforced by the database
- Selective denormalisation is applied on most of the 'narrow' tables, to
  allow for certain operations to be performed without a join, or to allow
  indexes to support some anticipated common operations. The columns added
  (where appropriate) are: user_id, created_at, to support trend and user
  counts respectively.
- A set of indexes is suggested to support analytical queries
- The directly collected table is intended to capture the case when a tweet is
  directly retrieved as part of an operation, so as to differentiate direct
  vs referential capture. It is modelled as an append only table, that can
  be joined with the main tweet table to construct the differentiation and to
  simplify the insert logic.
Examples of anticipated queries and purposes are shown along with each query,
and some more extended examples below.
*/
    user_id integer,
    context_id integer references collection_context,
    retrieved_at datetime,
    name text,
    profile_image_url text,
    created_at text,
    protected text,
    description text,
    location text,
    pinned_tweet_id integer,
    verified integer, -- boolean
    url text,
    username text,
    followers_count integer,
    following_count integer,
    tweet_count integer,
    listed_count integer,
    withheld_country_codes text,
    -- TODO: withheld related information
    primary key (user_id, retrieved_at)
) without rowid;

create table if not exists directly_collected_user(
    /*
    Records the IDs of users that were 'directly' collected as part of a collection.
    This allows distinguishing between a user that was included by directly from an
    an API call, or was only referenced by another collection.

    Examples:
    -- The number of directly collected users in this collection
    select
        count(*)
    from directly_collected_user;
    */
    user_id integer primary key
);

create table if not exists tweet_at_time (
    /*
    Contains the 'immutable' characteristics of a tweet, such as the text,
    structure of the conversation, the user ID and so on.
    Mutable characteristics of a tweet are captured in tweet metrics, to
    allow natural capture of change over time without complicating the
    main tweet table. Other aspects of tweets such as hashtags and
    mentions are treated as insert only tables of attributes to handle
    change over time.
    Examples:
    -- Count the number of tweets
    select count(*) from tweet;
    -- Select tweets that were 'directly' collected
    select count(*)
    from tweet
    inner join directly_collected_tweet using(tweet_id);
    -- Find the number of tweets and number of unique users
    -- by month that were directly collected
    select
        datetime(created_at, 'start of month') as month,
        count(distinct user_id) as active_users,
        count(*) as tweets
    from tweet
    inner join directly_collected_tweet using(tweet_id);
    -- Find the ten largest threads by number of tweets
    select
        conversation_id,
        count(*) as tweet_count
    from tweet
    group by conversation_id
    order by tweet_count desc limit 10;
    */
    tweet_id integer,
    context_id integer references collection_context,
    user_id integer,
    created_at text,
    retrieved_at datetime,
    conversation_id integer,
    retweeted_tweet_id integer,
    quoted_tweet_id integer,
    replied_to_tweet_id integer,
    text text,
    lang text,
    source text,
    possibly_sensitive integer, -- boolean
    reply_settings text,
    like_count integer,
    quote_count integer,
    reply_count integer,
    retweet_count integer,
    withheld_copyright integer, --boolean
    withheld_country_codes text,
    poll_id integer,
    place_id integer references place,
    primary key (tweet_id, retrieved_at),
    foreign key (user_id, retrieved_at) references user_at_time,
    foreign key (poll_id, retrieved_at) references poll
) without rowid;

create table if not exists directly_collected_tweet(
    /*
    Records the IDs of tweets that were 'directly' collected as part of a
    collection. This allows distinguishing between a tweet that was included
    by directly from an an API call, such as matching a search query, or was
    only referenced by another tweet such as by quoting.

    Examples:
    -- The number of directly collected tweets in this collection
    select
        count(*)
    from directly_collected_tweet;
    */
    tweet_id integer primary key
);

create table if not exists tweet_hashtag(
    tweet_id integer,
    retrieved_at datetime,
    hashtag text,
    foreign key (tweet_id, retrieved_at) references tweet_at_time,
    primary key (tweet_id, retrieved_at, hashtag)
) without rowid;

create table if not exists tweet_cashtag(
    tweet_id integer,
    retrieved_at datetime,
    cashtag text,
    foreign key (tweet_id, retrieved_at) references tweet_at_time,
    primary key (tweet_id, retrieved_at, cashtag)
) without rowid;

create table if not exists tweet_mention(
    tweet_id integer,
    retrieved_at datetime,
    mentioned_user_id text,
    mentioned_username text,
    foreign key (tweet_id, retrieved_at) references tweet_at_time,
    primary key (tweet_id, retrieved_at, mentioned_user_id)
) without rowid;

create table if not exists url(
    url text,
    retrieved_at datetime,
    description text,
    display_url text,
    expanded_url text,
    images text,
    media_key text,
    status text,
    title text,
    unwound_url text,
    primary key (url, retrieved_at)
) without rowid;

create table if not exists tweet_url(
    tweet_id integer,
    retrieved_at datetime,
    url text,
    foreign key (tweet_id, retrieved_at) references tweet_at_time,
    foreign key (url, retrieved_at) references url,
    primary key (tweet_id, retrieved_at, url)
) without rowid;

create table if not exists poll(
    poll_id integer,
    retrieved_at datetime,
    duration_minutes integer,
    end_datetime datetime,
    voting_status text,
    primary key (poll_id, retrieved_at)
) without rowid;

create table if not exists poll_option(
    poll_id integer,
    retrieved_at datetime,
    position integer,
    label text,
    votes integer,
    primary key (poll_id, retrieved_at, position),
    foreign key (poll_id, retrieved_at) references poll
) without rowid;

create table if not exists place(
    place_id text primary key,
    country text,
    country_code text,
    full_name text,
    geo_type text,
    geo_bbox_1 float,
    geo_bbox_2 float,
    geo_bbox_3 float,
    geo_bbox_4 float,
    name text,
    place_type text
) without rowid;

create table if not exists media(
    media_key text,
    retrieved_at datetime,
    alt_text text,
    duration_ms integer,
    preview_image_url text,
    view_count integer,
    type text,
    url text,
    width integer,
    height integer,
    primary key (media_key, retrieved_at)
) without rowid;

create table if not exists tweet_media(
    tweet_id integer,
    retrieved_at datetime,
    media_key text,
    primary key (tweet_id, retrieved_at, media_key)
    foreign key (media_key, retrieved_at) references media
) without rowid;

create table if not exists domain(
    domain_id primary key,
    name text,
    description text
);

create table if not exists entity(
    entity_id primary key,
    name text,
    description text
);

create table if not exists tweet_entity_domain(
    tweet_id integer,
    retrieved_at datetime,
    entity_id integer references entity,
    domain_id integer references domain,
    primary key (tweet_id, retrieved_at, entity_id, domain_id)
) without rowid;

create table if not exists user_matching_ruleset(
    ruleset_name text,
    user_id integer,
    primary key (ruleset_name, user_id)
);

create table if not exists user_ruleset_ngram_count(
    /* The empty string ruleset name will be interpreted as the global count
    of all profiles. */
    ruleset_name text,
    field text,
    first_token text,
    second_token text,
    third_token text,
    profile_count integer default 0,
    primary key (ruleset_name, field, first_token, second_token, third_token)
) without rowid;


create table if not exists metadata (
    key text primary key,
    value
);

create view if not exists user_latest as
    /* Note that this relies on the SQLite behaviour of selecting the row
       corresponding to the max value in the case of the group by. */
    select
        max(retrieved_at) as latest_retrieved_at,
        *
    from user_at_time
    group by user_id;

create view if not exists tweet_latest as
    /* Note that this relies on the SQLite behaviour of selecting the row
       corresponding to the max value in the case of the group by. */
    select
        max(retrieved_at) as latest_retrieved_at,
        *
    from tweet_at_time
    group by tweet_id;



insert or ignore into metadata values('twittersphere_schema_version', {});

""".format(
    CURRENT_SCHEMA_VERSION
)


table_keys = {
    "collection_context": "context_id",
    "user_at_time": "user_id,retrieved_at",
    "directly_collected_user": "user_id",
    "tweet_at_time": "tweet_id,retrieved_at",
    "directly_collected_tweet": "tweet_id",
    "tweet_hashtag": "tweet_id,retrieved_at,hashtag",
    "tweet_cashtag": "tweet_id,retrieved_at,cashtag",
    "tweet_mention": "tweet_id,retrieved_at,mentioned_user_id",
    "url": "url,retrieved_at",
    "tweet_url": "tweet_id,retrieved_at,url",
    "poll": "poll_id,retrieved_at",
    "poll_option": "poll_id,retrieved_at,position",
    "place": "place_id",
    "media": "media_key,retrieved_at",
    "tweet_media": "tweet_id,retrieved_at,media_key",
    "domain": "domain_id",
    "entity": "entity_id",
    "tweet_entity_domain": "tweet_id,retrieved_at,entity_id,domain_id",
    "metadata": "key",
}
