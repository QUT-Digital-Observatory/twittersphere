"""
The schema creation statements, and the associated index and materialised view
statements to be run after inserting data.

TODO:

- Likes, followers, following.

"""

CURRENT_SCHEMA_VERSION = 3

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
    pinned_tweet_id integer references tweet(tweet_id),
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
);

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
    user_id integer primary key references user_latest
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
    user_id integer references user_latest,
    created_at text,
    retrieved_at datetime,
    conversation_id text,
    retweeted_tweet_id text references tweet,
    quoted_tweet_id text references tweet,
    replied_to_tweet_id text references tweet,
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
    primary key (tweet_id, retrieved_at),
    foreign key (user_id, retrieved_at) references user_at_time
);

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
    tweet_id integer primary key references tweet
);

create table if not exists metadata (
    key text primary key,
    value
);

insert or ignore into metadata values('twittersphere_schema_version', {});

""".format(
    CURRENT_SCHEMA_VERSION
)

VIEWS_INDEXES = """

CREATE index if not exists tweet_period on tweet(created_at);
create index if not exists tweet_conversation on tweet(conversation_id);
-- Main tweet table

--
create index if not exists url_period on tweet_url(created_at);
create index if not exists url_user on tweet_url(user_id);
create index if not exists url_url on tweet_url(expanded_url);
create index if not exists url_domain on tweet_url(expanded_domain);

--
create index if not exists hashtag_tweet on tweet_hashtag(hashtag);
create index if not exists hashtag_normalised_tweet on tweet_hashtag(hashtag_normalised);
create index if not exists hashtag_period on tweet_hashtag(created_at);

--
create index if not exists user_mentioning on tweet_mention(user_id, mentioned_user_id);
create index if not exists user_mentioned on tweet_mention(mentioned_user_id, user_id);

"""
