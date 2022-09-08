# Twittersphere

Twittersphere is a tool for ingesting tweets and applying content based
filtering rules for tweets and user profiles. Twittersphere uses simple
inclusion/exclusion rules based on words and phrases to include or exclude
user profiles or tweets as belonging to part of a group.


## Functionality

Twittersphere exposes a command line interface and a Python library for:

- Extracting relevant tweet entities from Twitter V2 API JSON
- Creating a structured relational database in SQLite for further analytics
- Applying rule based filters to select user profiles based on their content
- Iteratively create or update these rule based filters


## Command Line Usage

In the first instance Twittersphere can be used to create a local relational
database from files containing V2 API Twitter JSON data collected via twarc.
Any tweet or user JSON data collected via the Twitter API including search
and streaming endpoints should work. Note that this process safely
deduplicates items: you can insert the same file more than once and not see
the same tweet twice.

```
twittersphere prepare processed.db FILE1.json FILE2.json ... FILEN.json
```

An existing ruleset (such as ... this not yet public Australian Twittersphere
rules ...) can be applied as follows:

```
twittersphere filter-users processed.db oz_twittersphere_rules.csv
```

This will create a new table `...` which indicates which user profiles in the
database have matched the rules in the database. Note that matching users are
tagged with the filename of the ruleset - multiple rulesets can be applied
and tracked.


### Updating Rules

After applying a ruleset, you can generate an updated list of rules with new
candidate rules to expand the existing matching population. Note that the first
time you run this command will take longer, as this is when initial statistics
about ngrams are created.

The following command wil
```
twittersphere refine-user-rules processed.db RULESET_NAME candidate_rules.csv
```

Note that the following will show you which rules have already been applied
and are valid rules for `RULESET_NAME`:

```
twittersphere list-user-rules processed.db
```

### Creating Rules

To create a new rules you will need to generate an initial seed rule set, or
alternatively an initial seed population set.


## Limitations

Note that Twittersphere does not support Twitter V1.1 data at all.

Data collected with tools other than twarc, collected with twarc metadata
turned off, or collected with limited fields included in the output will not
be well supported.

