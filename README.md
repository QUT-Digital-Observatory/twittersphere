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

### Creating a database

In the first instance Twittersphere can be used to create a local relational
database from files containing V2 API Twitter JSON data collected via twarc.
Any tweet or user JSON data collected via the Twitter API including search
and streaming endpoints should work. Note that this process safely
deduplicates items: you can insert the same file more than once and not see
the same tweet twice. This database can be queried directly from most
programming languages, or after installing an [ODBC connector](http://www.ch-werner.de/sqliteodbc/)
can be connected to tools like Excel or Tableau.

```
twittersphere prepare FILE1.json FILE2.json ... FILEN.json processed.db
```

### Rule Based User Filtering

An existing ruleset (such as ... this not yet public Australian Twittersphere
rules ...) can be applied as follows:

```
twittersphere filter-users rules.csv processed.db
```

This will populate the `user_matching_ruleset` table with the `user_id`'s of
profiles that have matched that ruleset, along with the name of the filename
of the rules for later reference.


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

