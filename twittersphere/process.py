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

# This is for entities.hashtags
HASHTAGS = ["tag"]
TWEET_MENTIONS = [{"username": "username", "user_id": "id"}]


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
            # Grab the first poll id only - currently a tweet can only have one
            # poll, but the data structure implies 1:many relationship
            "poll_id": Coalesce(("attachments.poll_ids", T[0]), default=None),
            "place_id": Coalesce("geo.place_id", default=None),
            "media_keys": Coalesce("attachments.media_keys", default=[]),
            "hashtags": (Coalesce("entities.hashtags", default=[]), HASHTAGS),
            "mentions": (Coalesce("entities.mentions", default=[]), TWEET_MENTIONS),
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

POLL_SPEC = {
    "duration_minutes": "duration_minutes",
    "end_datetime": "end_datetime",
    "id": "id",
    "options": "options",
    "voting_status": "voting_status",
}


# "entities.annotations": "entities.annotations",
# "entities.cashtags": "entities.cashtags",
# "entities.urls": "entities.urls",
# "context_annotations": "context_annotations",


PLACE_SPEC = {
    "country": "country",
    "country_code": "country_code",
    "full_name": "full_name",
    "geo_type": "geo.type",
    "geo_bbox_1": ("geo.bbox", T[0]),
    "geo_bbox_2": ("geo.bbox", T[1]),
    "geo_bbox_3": ("geo.bbox", T[2]),
    "geo_bbox_4": ("geo.bbox", T[3]),
    # TODO: investigate how important the properties are
    "id": "id",
    "name": "name",
    "place_type": "place_type",
}

MEDIA_SPEC = {
    "alt_text": Coalesce("alt_text", default=None),
    "duration_ms": Coalesce("duration_ms", default=None),
    "media_key": "media_key",
    "preview_image_url": Coalesce("preview_image_url", default=None),
    "view_count": Coalesce("view_count", default=None),
    "type": "type",
    "url": Coalesce("url", default=None),
    "width": "width",
    "height": "height",
}

METADATA_SPEC = {
    "twarc_version": "version",
    "retrieved_at": "retrieved_at",
    "twitter_url": "url",
}

# This is a composition of all of the individual specs into a single glom spec
# that should work on the different variations of responses from the Twitter API.
PAGE_SPEC = {
    "metadata": ("__twarc", METADATA_SPEC),
    # Note that only one of these can match, the main payload can't be of mixed type.
    "data": Coalesce(
        # User data type
        ({"users": ("data", [USER_SPEC])}),
        # Array of tweets (search, timelines etc)
        ({"tweets": ("data", [TWEET_SPEC])}),
        # Single tweet - sample/filter
        ({"tweets": ("data", TWEET_SPEC, lambda x: [x])}),
        # Note that no default is specified here - if nothing matches an error needs to be raised
        # because otherwise we don't know what the data actually is.
    ),
    "includes": {
        "users": (Coalesce("includes.users", default=[]), [USER_SPEC]),
        "tweets": (Coalesce("includes.tweets", default=[]), [TWEET_SPEC]),
        "polls": (Coalesce("includes.polls", default=[]), [POLL_SPEC]),
        "places": (Coalesce("includes.places", default=[]), [PLACE_SPEC]),
        "media": (Coalesce("includes.media", default=[]), [MEDIA_SPEC]),
    },
}


def process_page(raw_page):

    page = json.loads(raw_page)

    return glom(page, PAGE_SPEC)
