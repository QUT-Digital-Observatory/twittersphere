import csv
import json

import click

from . import db
from . import twittersphere as twt


@click.group()
@click.pass_context
def twittersphere(ctx):
    """
    Twittersphere
    """


@twittersphere.command("prepare")
@click.argument("input_files", type=click.Path(exists=True), nargs=-1)
@click.argument("output_db", type=click.Path())
@click.option("--n-cpus", default=2, type=click.IntRange(min=1))
@click.option(
    "--max-memory-db-size",
    default=2,
    type=click.FloatRange(min=0.1),
    help="The maximum size of the in memory staging database in GiB. "
    "Using more memory may speed up jobs with 10's of millions of tweets.",
)
@click.pass_context
def prepare(ctx, input_files, output_db, n_cpus, max_memory_db_size):
    """
    Processes the Twitter V2 JSON input_files into the output_db.

    """

    def iterate_pages(input_files):
        for input_file in input_files:
            with open(input_file, "rb") as f:
                for line in f:
                    yield line

    max_db_size = max_memory_db_size * (2**30)
    db.insert_pages(
        output_db,
        iterate_pages(input_files),
        n_cpus=n_cpus,
        in_memory_max_db_size=max_db_size,
    )


@twittersphere.command("filter-users")
@click.argument("rule-file", type=click.Path(exists=True))
@click.argument("prepared-db", type=click.Path(exists=True))
@click.option("--ruleset-name", default=None, type=str)
# @click.option("--qa-profile-count", type=int, default=100)
@click.pass_context
def filter_users(ctx, rule_file, prepared_db, ruleset_name):
    """
    Apply the given ruleset to user profiles in `prepared-db`.

    The latest `retrieved-at` version of each user profile will be used for
    labelling. The `user_id` of each matching profile will be inserted
    into the `user_matching_ruleset` table with the name of the ruleset.
    This can be customised with the `--ruleset-name`, or if not provided,
    the filename of the `rule-file` will be used instead.

    If `ruleset_name` already exists in the table, it will be replaced.

    """

    db_conn = db.ensure_db(prepared_db)
    include_rules, exclude_rules, _ = twt.load_rule_files([rule_file])

    twt.apply_user_rules_to_db(
        db_conn, include_rules, exclude_rules, ruleset_name or rule_file
    )


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

        bit_location = 2**16

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
