"""
These tests assume that twitter data has already been collected using tox -e
collect_test_data - the data is not stored in the repository to comply with
the Twitter terms of service and it's also necessary to use different
endpoints to capture the different shapes of API responses so rehydration
won't be helpful.

Note that the testing strategy here is close to end-to-end testing, exercising
the CLI as entrypoints for the functionality of the package.

"""
import pathlib
import sqlite3

from click.testing import CliRunner

from twittersphere import cli

data_path = pathlib.Path("tests", "data")


def test_prepare_filter_db(tmp_path):

    target_db = tmp_path / "test.db"

    runner = CliRunner()

    result = runner.invoke(
        cli.twittersphere,
        [
            "prepare",
            str(data_path / "test_sample.json"),
            str(data_path / "test_search_recent.json"),
            str(data_path / "test_hydrated_profiles.json"),
            str(target_db),
        ],
    )
    assert result.exit_code == 0

    db = sqlite3.connect(target_db)
    tweet_count = list(db.execute("select count(*) from directly_collected_tweet"))[0][
        0
    ]
    assert tweet_count >= 2000

    direct_user_count = list(
        db.execute("select count(*) from directly_collected_user")
    )[0][0]
    total_user_count = list(
        db.execute("select count(distinct user_id) from user_at_time")
    )[0][0]

    with open(data_path / "user_ids.txt", "r") as f:
        actual_user_count = len({l for l in f})

    # Note that users can be suspended in between collection and hydration!
    assert 0 < direct_user_count <= actual_user_count
    assert direct_user_count < total_user_count

    # Apply the simple "australia" rule to the database
    result = runner.invoke(
        cli.twittersphere,
        [
            "filter-users",
            str(data_path / "example_rule.csv"),
            str(target_db),
            "--ruleset-name",
            "test",
        ],
    )

    assert result.exit_code == 0
    matched_count = list(
        db.execute(
            """
            select count(*)
            from user_matching_ruleset
            where ruleset_name = 'test'
            """
        )
    )[0][0]

    assert matched_count > 0


# Test applying the rules to the database, and to the streams of tweets
# Test extracting the qa profiles?
# Test applying the labelled qa profiles.
