"""
Utilities for extracting tweet related entities from V2 Twitter API JSON as
collected via twarc.

The data processing for this module is built around a set of
[Glom](https://glom.readthedocs.io/en/latest/) specifications for the individual
little pieces of the API data.

Note that the functionality here works with one page of API data at a time -
when processing lots of data you will need to consider how you will handle
problems like users and tweets duplicated over time/API responses.

"""

from collections import defaultdict
import json

from glom import glom, Coalesce, Merge, T


# entities.description.cashtags
# entities.description.hashtags
# entities.description.mentions
# entities.description.urls
# entities.url.urls

USER_SPEC = {
    "id": "id",
    "created_at": "created_at",
    "username": "username",
    "name": "name",
    "description": "description",
    "location": Coalesce("location", default=None),
    "pinned_tweet_id": Coalesce("pinned_tweet_id", default=None),
    "profile_image_url": "profile_image_url",
    "protected": "protected",
    "followers_count": "public_metrics.followers_count",
    "following_count": "public_metrics.following_count",
    "listed_count": "public_metrics.listed_count",
    "tweet_count": "public_metrics.tweet_count",
    "url": "url",
    "verified": "verified",
    "withheld_country_codes": Coalesce(
        ("withheld.country_codes", json.dumps), default=None
    ),
}

# This unpacks the list of dicts to a single dict,
# then generates nulls for not present values.
REFERENCED_TWEET_SPEC = (
    [{T["type"]: T["id"]}],
    Merge(),
    {
        "replied": Coalesce("replied", default=None),
        "retweeted": Coalesce("retweeted", default=None),
        "quoted": Coalesce("quoted", default=None),
    },
)


TWEET_SPEC = (
    # Because of the structure of the referenced_tweets key,
    # process this into two dicts, that will then be merge later
    # into a single object.
    {
        "all": {
            "id": "id",
            "conversation_id": "conversation_id",
            "author_id": "author_id",
            "created_at": "created_at",
            "text": "text",
            "lang": "lang",
            "source": "source",
            "like_count": "public_metrics.like_count",
            "quote_count": "public_metrics.quote_count",
            "reply_count": "public_metrics.reply_count",
            "retweet_count": "public_metrics.retweet_count",
            "reply_settings": "reply_settings",
            "possibly_sensitive": "possibly_sensitive",
            "withheld_copyright": Coalesce("withheld.copyright", default=None),
            "withheld_country_codes": Coalesce(
                ("withheld.country_codes", json.dumps), default=None
            ),
            "place_id": Coalesce("geo.place_id", default=None),
            "poll_id": Coalesce("attachments.poll.id", default=None),
        },
        "referenced": Coalesce(
            ("referenced_tweets", REFERENCED_TWEET_SPEC),
            default={"replied": None, "retweeted": None, "quoted": None},
        ),
    },
    # Unpack the subdictionaries
    T.values(),
    # Then merge them together
    Merge(),
)


# "entities.annotations": "entities.annotations",
# "entities.cashtags": "entities.cashtags",
# "entities.hashtags": "entities.hashtags",
# "entities.mentions": "entities.mentions",
# "entities.urls": "entities.urls",
# "context_annotations": "context_annotations",
# "attachments.media": "attachments.media",
# "attachments.media_keys": "attachments.media_keys",
# "attachments.poll.duration_minutes": "attachments.poll.duration_minutes",
# "attachments.poll.end_datetime": "attachments.poll.end_datetime",
# "attachments.poll.id": "attachments.poll.id",
# "attachments.poll.options": "attachments.poll.options",
# "attachments.poll.voting_status": "attachments.poll.voting_status",
# "attachments.poll_ids": "attachments.poll_ids",
# "geo.coordinates.coordinates": "geo.coordinates.coordinates",
# "geo.coordinates.type": "geo.coordinates.type",
# "geo.country": "geo.country",
# "geo.country_code": "geo.country_code",
# "geo.full_name": "geo.full_name",
# "geo.geo.bbox": "geo.geo.bbox",
# "geo.geo.type": "geo.geo.type",
# "geo.id": "geo.id",
# "geo.name": "geo.name",
# "geo.place_id": "geo.place_id",
# "geo.place_type": "geo.place_type",

METADATA_SPEC = {
    "twarc_version": "version",
    "retrieved_at": "retrieved_at",
    "twitter_url": "url",
}

PAGE_SPEC = {
    "metadata": ("__twarc", METADATA_SPEC),
    # Note that only one of these can match, the main payload can't be of mixed type.
    "data": Coalesce(
        # User data type
        ({"users": ("data", [USER_SPEC])}),
        # Array of tweets (search, timelines etc)
        ({"tweets": ("data", [TWEET_SPEC])}),
        # Single tweet - sample/filter
        ({"tweets": ("data", TWEET_SPEC)}),
        # Note that no default is specified here - if nothing matches an error needs to be raised
        # because otherwise we don't know what the data actually is.
    ),
    "includes": {
        "users": Coalesce(("includes.users", [USER_SPEC]), default=[]),
        "tweets": Coalesce(("includes.tweets", [TWEET_SPEC]), default=[]),
    },
}


def process_page(raw_page):

    page = json.loads(raw_page)

    return glom(page, PAGE_SPEC)


def extract_includes(includes):
    """
    Process the "includes" payload of a page of responses.

    The "includes" payload is much more consistent than the main payload, so
    we can just dispatch without thinking about it. Note also that the "data"
    payload can only ever contain tweets, users, or lists - media and places
    never occur.

    """
    processed = {
        "tweets": extract_tweets(includes.get("tweets", [])),
        "users": extract_users(includes.get("users", [])),
    }

    return processed


def extract_main(data):
    """
    Extract the main data component from the page of respones.

    Automatically determines the type of payload (typically users
    or tweets), and dispatches as appropriate.

    """

    if isinstance(data, dict):
        # This is a stream of tweets
        return {"tweets": extract_tweets(data)}

    elif "author_id" in data[0]:
        # This is also tweets
        return {"tweets": extract_tweets(data)}
    else:
        return {"users": extract_users(data)}


def extract_users(users_component):
    return glom(users_component, [USER_SPEC])


def extract_tweets(tweet_component):
    return []
