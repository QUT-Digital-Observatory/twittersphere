"""
The core of the twittersphere package for ingesting tweets and creating or
applying content based rulesets.

"""
from collections import defaultdict, Counter
import csv
import json
import random
import sys

import click
import regex


token_regex = regex.compile(
    r"\b\p{Word_Break=WSegSpace}*['\u200D]?",
    flags=regex.WORD | regex.UNICODE | regex.V1,
)

stopwords = set(
    ", . ' \" - + & / | : ; \r\n \n \t \r ( ) ! * ? ] [ ^ ~ $ % { } =".split(" ")
)
stopwords.add("")


def tokenize(text, stopwords):
    """Return a list of tokens in text, after removing common punctuation."""
    return [t for t in token_regex.split(text.lower()) if t not in stopwords]


def ngrams(text, n, stopwords):
    """
    Return all ngrams up to size n in the string.

    Stopwords are removed from the stream of tokens before ngrams are generated.

    """
    token_list = tokenize(text, stopwords)
    n_tokens = len(token_list)
    found_ngrams = []

    for l in range(1, n + 1):

        offset = l - 1

        for i in range(0, n_tokens - offset):
            ngram = token_list[i : i + l]
            found_ngrams.append(tuple(ngram))

    return set(found_ngrams)


def deduplicate_rules(rules):
    """
    Given a set of rules that consist of ngrams, reduce them down to a minimal set.

    If the single token ('Australia',) is a rule, then even though
    ('Brisbane', 'Australia') is also a match, it is redundant. This procedures removes
    redundant rules of length n + 1 ththatat are already matched by rules of length n.

    Deduplication is necessary so we can properly account for multiple includes and
    excludes in the same tweet.
    """

    why_redundant = defaultdict(set)

    # Yeah, if you want to do longer length rules this should be properly broken down...
    for rule in rules:
        if len(rule) == 2:
            sub_rules = [tuple(rule)]
            # Check unigrams
            if rule[:1] in rules:
                why_redundant[rule[:1]].add(rule)
            if rule[1:] in rules:
                why_redundant[rule[1:]].add(rule)

        elif len(rule) == 3:
            # Check unigrams
            if rule[:1] in rules:
                why_redundant[rule[:1]].add(rule)
            if rule[1:2] in rules:
                why_redundant[rule[1:2]].add(rule)
            if rule[2:] in rules:
                why_redundant[rule[2:]].add(rule)

            # Check bigrams
            if rule[:2] in rules:
                why_redundant[rule[:2]].add(rule)
            if rule[1:] in rules:
                why_redundant[rule[1:]].add(rule)

    redundant_rules = set(rule for leaves in why_redundant.values() for rule in leaves)

    return rules - redundant_rules, why_redundant


def load_rule_files(file_paths):
    """
    Load a set of rules from the given files.

    A rule file is a CSV file containing at least the following fields
    in any order, with the following values:

    - include: 1, -1 or blank.

        1 includes that this is a positive matching rule, -1 a negative
        matching rule, blank for not a match. Blank fields won't affect
        the results, but are retained in the file to avoiding annotating
        rules again.

    - field: 'location', 'description' or 'realname'

        The field to match on.

    - first_token, second_token, third_token: strings

        tokens to match on as part of the rule. Single/double
        word matches should start on first_token and leave unneeded
        tokens blank.

    """
    redundant_include_rules = defaultdict(set)
    redundant_exclude_rules = defaultdict(set)

    # Used to not present rows that have already been presented to users for labelling,
    # and for ensuring that redundant labels are carried through always.
    all_rules = defaultdict(dict)

    for file in file_paths:
        with open(file, "r") as f:
            reader = csv.DictReader(f)

            fields = ["first_token", "second_token", "third_token"]

            for row in reader:

                ngram = tuple([row[f] for f in fields if row[f]])

                include = row["include"]
                field = row["field"]

                if include == "1":
                    redundant_include_rules[field].add(ngram)

                elif include == "-1":
                    redundant_exclude_rules[field].add(ngram)

                elif include:
                    raise ValueError(f"Unknown include column value {include}")

                all_rules[field][ngram] = include

                # Ensure the field exists in every defaultdict for downstream
                redundant_include_rules[field]
                redundant_exclude_rules[field]

    # Prune any exclude rules that don't have a subcomponent that doesn't match an
    # include rule. Note this needs to be done before deduplication.
    prune_rules = defaultdict(set)

    for field, rules in redundant_exclude_rules.items():
        for rule in rules:
            # By definition there's no associated positive rule
            if len(rule) == 1:
                prune_rules[field].add(rule)
            else:
                matched = False
                for token in rule:
                    if (token,) in redundant_include_rules[field]:
                        matched = True
                        break

                if len(rule) == 3 and (
                    rule[:2] in redundant_include_rules[field]
                    or rule[1:] in redundant_include_rules[field]
                ):
                    matched = True

                if not matched:
                    prune_rules[field].add(rule)

    for field, rules in prune_rules.items():
        redundant_exclude_rules[field] -= rules

    include_rules = {
        field: deduplicate_rules(rules)[0]
        for field, rules in redundant_include_rules.items()
    }

    exclude_rules = {
        field: deduplicate_rules(rules)[0]
        for field, rules in redundant_exclude_rules.items()
    }

    return include_rules, exclude_rules, all_rules


def classify_user_profile(profile, include_rules, exclude_rules):
    """
    Apply the rules to the given profile.

    Returns the boolean match, and which rules matched, although the
    later is not currently used.
    """

    positive_matches = defaultdict(set)
    negative_matches = defaultdict(set)

    for field in ("name", "description", "location"):

        text = profile.get(field, "")

        ngram_tokens = ngrams(text, 3, stopwords=stopwords)
        positive_matches[field] = ngram_tokens & include_rules.get(field, set())
        negative_matches[field] = ngram_tokens & exclude_rules.get(field, set())

    matched = sum(len(m) for m in positive_matches.values()) > sum(
        len(m) for m in negative_matches.values()
    )

    return matched, positive_matches, negative_matches


def apply_rules(api_result_page, include_rules, exclude_rules):
    """
    Take a set of rules, and apply it to a page of V2 Twitter API data.

    Returns a new JSON document, containing the subset of tweets (or accounts)
    directly authored by a profile considered to match the given rules.

    Also returns data structures that allow accumulating counts of tweets by
    user_id for the purpose of understanding distributions and quantifying
    effectiveness.

    """

    matched_author_ids = set()
    transformed_result_page = api_result_page.copy()

    # Note the granularity here is author_id -> profile fields.
    # There is only one user object per page in the API responses,
    # but this is not the right level for the total evaluation once
    # results from pages are merged.
    account_tweet_count = dict()

    # Tweets have authors, user profile objects don't
    if "author_id" in api_result_page["data"][0]:

        # Authored tweets - we need to extract this here, as we're currently only labelling
        # top level collected tweets, not tweets included by reference.
        top_level_authors = set(tweet["author_id"] for tweet in api_result_page["data"])

        for profile in api_result_page["includes"]["users"]:

            author_id = profile["id"]

            # Skip user objects for authors of referenced tweets.
            if author_id not in top_level_authors:
                continue

            matched, positive_matches, negative_matches = classify_user_profile(
                profile, include_rules, exclude_rules
            )

            author_fields = tuple(
                profile.get(field, "") for field in ("name", "description", "location")
            )

            if matched:
                matched_author_ids.add(author_id)
                account_tweet_count[(True, author_id)] = [author_fields, 0]
            else:
                account_tweet_count[(False, author_id)] = [author_fields, 0]

        transformed_result_page["data"] = []

        for tweet in api_result_page["data"]:

            author_id = tweet["author_id"]

            if author_id in matched_author_ids:

                transformed_result_page["data"].append(tweet)
                account_tweet_count[(True, author_id)][1] += 1

            else:
                account_tweet_count[(False, author_id)][1] += 1

    # User profiles
    else:
        raise ValueError("Only tweets are currently supported for filtering")

    return transformed_result_page, account_tweet_count


def transform_profiles_for_annotation(
    account_tweet_count, qa_profile_count, sampling="class_stratified"
):
    if qa_profile_count < 0:
        raise ValueError(
            f"qa_profile_count should be positive, instead it was {qa_profile_count}."
        )

    if sampling == "class_stratified":
        matched_profile_lines = [
            key for key, (_, _, matched) in account_tweet_count.items() if matched
        ]
        not_matched_profile_lines = [
            key for key, (_, _, matched) in account_tweet_count.items() if not matched
        ]

        sample_profiles = random.sample(
            matched_profile_lines, qa_profile_count // 2
        ) + random.sample(not_matched_profile_lines, qa_profile_count // 2)

    else:
        sample_profiles = random.sample(
            list(account_tweet_count.keys()), qa_profile_count
        )

    random.shuffle(sample_profiles)

    bit_location = 2**16

    for profile in sample_profiles:
        author_ids, tweet_count, matched = account_tweet_count[profile]

        # We're going to obfuscate the matched column with a random number so
        # it doesn't leak into labelling
        obfuscated_match = random.randint(0, sys.maxsize)
        if matched:
            obfuscated_match |= bit_location
        else:
            obfuscated_match & ~bit_location

        yield (*profile, len(author_ids), tweet_count, hex(obfuscated_match))


def apply_user_rules_to_db(db_conn, include_rules, exclude_rules, ruleset_name):
    """
    Apply the specified include/exclude rules to the given database conn.

    Only the latest version of each profile is considered for labelling.

    """
    try:
        db_conn.execute("begin")

        db_conn.execute(
            "delete from user_matching_ruleset where ruleset_name = ?", [ruleset_name]
        )

        profiles = db_conn.execute(
            "select user_id, location, description, name from user_latest"
        )

        for user_id, location, description, name in profiles:
            matched, _, _ = classify_user_profile(
                {
                    "location": location or "",
                    "description": description or "",
                    "name": name or "",
                },
                include_rules,
                exclude_rules,
            )

            if matched:
                db_conn.execute(
                    "insert into user_matching_ruleset values (?, ?)",
                    [ruleset_name, user_id],
                )

        db_conn.execute("commit")

    except:
        db_conn.execute("rollback")
        raise


def count_user_ngrams(db_conn, stopwords, ruleset_name=""):
    """
    Count ngrams present in the latest version of each user in the collection.

    If no ruleset_name is provided, this will be a global count across all
    matching users, otherwise only the `user_id`'s that match `ruleset_name`
    in the `user_matching_ruleset` table will be counted. This is to support
    statistical comparisons across different ruleset groups without
    tokenising everything again.

    """

    try:
        db_conn.execute("begin")

        db_conn.execute(
            """
            create temporary table ruleset_ngram_part as
                select *
                from user_ruleset_ngram_count
                limit 0
            """
        )

        if ruleset_name:
            profiles = db_conn.execute(
                """
                select
                    user_id, location, description, name
                from user_latest
                where user_id in (
                    select user_id
                    from user_matching_ruleset
                    where ruleset_name = ?
                )
                """,
                [ruleset_name],
            )
        else:
            profiles = db_conn.execute(
                """
                select
                    user_id, location, description, name
                from user_latest
                """
            )

        ngram_counts = defaultdict(Counter)

        counts = 0

        for user_id, location, description, name in profiles:

            # Tokenise all of the fields.
            if location:

                l = ngrams(location, 3, stopwords=stopwords)
                counts += len(l)

                for ngram in l:
                    ngram_counts["location"][ngram] += 1

            if description:

                d = ngrams(description, 3, stopwords=stopwords)
                counts += len(d)

                for ngram in d:
                    ngram_counts["description"][ngram] += 1

            if name:

                n = ngrams(name, 3, stopwords=stopwords)
                counts += len(n)

                for ngram in n:
                    ngram_counts["name"][ngram] += 1

            if counts >= 1000000:

                db_conn.executemany(
                    """insert into ruleset_ngram_part values(?, ?, ?, ?, ?, ?)""",
                    (
                        (ruleset_name, field, *(n + (("",) * (3 - len(n)))), count)
                        for field, values in ngram_counts.items()
                        for n, count in values.items()
                    ),
                )

                ngram_counts = defaultdict(Counter)
                counts = 0

        # Make sure to get that final batch.
        db_conn.executemany(
            """insert into ruleset_ngram_part values(?, ?, ?, ?, ?, ?)""",
            (
                (ruleset_name, field, *(n + (("",) * (3 - len(n)))), count)
                for field, values in ngram_counts.items()
                for n, count in values.items()
            ),
        )

        # Make sure to clear out old ngrams first
        db_conn.execute(
            "delete from user_ruleset_ngram_count where ruleset_name = ?",
            [ruleset_name],
        )

        # Aggregate the new parts into the table.
        db_conn.execute(
            """
            insert into user_ruleset_ngram_count
                select
                    ruleset_name,
                    field,
                    first_token,
                    second_token,
                    third_token,
                    sum(profile_count)
                from ruleset_ngram_part
                group by 1, 2, 3, 4, 5
            """
        )

        db_conn.execute("drop table ruleset_ngram_part")
        db_conn.execute("commit")

    except:
        db_conn.execute("rollback")
        db_conn.execute("drop table ruleset_ngram_part")
        raise
