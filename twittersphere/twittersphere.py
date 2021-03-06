"""
twittersphere.py

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
        positive_matches[field] = ngram_tokens & include_rules[field]
        negative_matches[field] = ngram_tokens & exclude_rules[field]

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

    bit_location = 2 ** 16

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


@click.group()
@click.pass_context
def twittersphere(ctx):
    """
    Twittersphere
    """


@twittersphere.command("filter")
@click.argument("input_files", type=click.Path(exists=True), nargs=-1)
@click.argument("output_file", type=click.Path(exists=False))
@click.option("--rules", type=click.Path(exists=True))
@click.option("--qa-profile-count", type=int, default=100)
@click.pass_context
def filter(ctx, input_files, output_file, rules, qa_profile_count):

    # Instantiate and validate the rules
    include_rules, exclude_rules, all_rules = load_rule_files([rules])
    total_include_rules = sum(len(x) for x in include_rules.values())
    total_exclude_rules = sum(len(x) for x in exclude_rules.values())

    print(
        f"Loaded {total_include_rules} distinct inclusion rules "
        f"and {total_exclude_rules} distinct exclusion rules from {rules}"
    )

    tweets_seen = 0
    tweets_matched = 0
    next_status = 10000

    with open(output_file, "w") as out:

        account_tweet_count = dict()

        for file in input_files:
            with open(file, "r") as f:
                for line in f:
                    api_result_page = json.loads(line)
                    transformed_result_page, page_account_tweet_counts = apply_rules(
                        api_result_page, include_rules, exclude_rules
                    )

                    tweets_seen += len(api_result_page["data"])
                    tweets_matched += len(transformed_result_page["data"])

                    # Merge the count of tweets by account. We keep track of the distinct
                    # versions of the profile information for reporting purposes.
                    for (
                        (matched, author_id),
                        (profile_fields, tweet_count),
                    ) in page_account_tweet_counts.items():

                        if profile_fields in account_tweet_count:
                            account_tweet_count[profile_fields][0].add(author_id)
                            account_tweet_count[profile_fields][1] += tweet_count

                        else:
                            account_tweet_count[profile_fields] = [
                                set([author_id]),
                                tweet_count,
                                matched,
                            ]

                    if tweets_seen >= next_status:
                        print(f"Matched {tweets_matched}/{tweets_seen}.")

                        next_status += 10000

                    out.write(json.dumps(transformed_result_page))
                    out.write("\n")

    print(f"Matched {tweets_matched}/{tweets_seen} in total.")

    qa_path = f"{output_file}.qa.csv"
    print(f"Saving QA data to {qa_path}")

    with open(qa_path, "w") as qa:
        writer = csv.writer(qa, quoting=csv.QUOTE_ALL, dialect="excel")

        writer.writerow(
            [
                "name",
                "description",
                "location",
                "unique_profiles",
                "tweet_count",
                "obfuscated_match",
                "human_label",
            ]
        )
        for row in transform_profiles_for_annotation(
            account_tweet_count, qa_profile_count
        ):
            writer.writerow(row)


@twittersphere.command("concordance")
@click.argument("input_file", type=click.Path(exists=True))
@click.pass_context
def concordance(ctx, input_file):
    """
    Measure concordance between the automated labels and the human labels.

    TODO:

    The output file deobfuscates the machine matched column and rules, and
    creates additional columns describing the entries in the concordance
    table.

    """
    with open(input_file, "r") as f:
        reader = csv.reader(f, quoting=csv.QUOTE_ALL, dialect="excel")

        # skip header
        next(reader)

        bit_location = 2 ** 16

        # Accumulators
        concordance = Counter()
        weighted_concordance = Counter()

        for row in reader:
            profile_count = int(row[3])
            tweet_count = int(row[4])
            auto_match = bool(int(row[5], 0) & bit_location)
            human_match = bool(row[6])

            concordance[(human_match, auto_match)] += profile_count
            weighted_concordance[(human_match, auto_match)] += tweet_count

        print(
            "Profile weighted concordance counts. Format is Human/Auto decision: weight."
        )
        for decision, weight in sorted(concordance.items()):
            print(decision, weight)

        print("Tweet weighted concordance counts.")
        for decision, weight in sorted(weighted_concordance.items()):
            print(decision, weight)
