from twittersphere import _schema


def test_schema_prepare():
    """
    Make sure each created table is present in the table_keys that drives the flush.

    """
    tables_created = _schema.SCHEMA_STATEMENTS.lower().count("create table")
    assert len(_schema.table_keys) == tables_created
