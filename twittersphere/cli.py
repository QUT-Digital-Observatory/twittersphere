import csv
import json

import click

from . import db


@click.group()
@click.pass_context
def twittersphere(ctx):
    """
    Twittersphere
    """


@twittersphere.command("prepare")
@click.argument("input_files", type=click.Path(exists=True), nargs=-1)
@click.argument("output_db", type=click.Path())
@click.option("--n_cpus", default=4, type=click.IntRange(min=1))
@click.pass_context
def prepare(ctx, input_files, output_db, n_cpus):
    """
    Processes the Twitter V2 JSON input_files into the output_db.

    """

    conn = db.ensure_db(output_db)

    for input_file in input_files:
        with open(input_file, "r") as f:
            db.insert_pages(conn, f, n_cpus=n_cpus)


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
