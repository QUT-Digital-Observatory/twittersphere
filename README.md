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

An existing ruleset can be applied as follows:

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


## Existing rule sets

Do you have a set of rules you've built with `twittersphere`? Let us know by submitting an [issue](https://github.com/QUT-Digital-Observatory/twittersphere/issues), posting in the [discussions](https://github.com/QUT-Digital-Observatory/twittersphere/discussions), or emailing [digitalobservatory@qut.edu.au](mailto:digitalobservatory@qut.edu.au) and we can add it to this list. Or feel free to submit a pull request if you'd like to add it yourself!

### The Australian Twittersphere

[The Australian Twittersphere](https://www.digitalobservatory.net.au/resources/australian-twittersphere/) is a longitudinal (2016-2023) collection of tweets from a population of Twitter accounts which explicitly self-identify as Australian or having connection to Australia. The population list (and rule list) and the collection of tweets itself are operated as a databank by the [QUT Digital Observatory](https://www.digitalobservatory.net.au/).

The [Australian Twittersphere rule list](https://researchdatafinder.qut.edu.au/display/n41990) is published as open data as a file compatible with `twittersphere`. You can use it to determine whether a Twitter/X profile would be eligible for inclusion in the Australian Twittersphere.

These population rules can be cited as:

> Hames, Sam; Takahashi, Marissa; Miller, Alice; QUT Digital Observatory; (2023): Australian Twittersphere Population Rules. Queensland University of Technology. (Dataset) https://doi.org/10.25912/RDF_1711599518062

## Limitations

Note that Twittersphere does not support Twitter V1.1 data at all. No support is provided for any API changes made by X after June 2023.

Data collected with tools other than twarc, collected with twarc metadata
turned off, or collected with limited fields included in the output will not
be well supported.

