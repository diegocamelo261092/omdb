# What's on the movies?

The overall idea of this project is to retrieve movies being displayed in the local cinema and return back the relevant information such as the IMDB ratings.

# Project Stages

## Movie Ratings

### IMDB's IDs
Retrieve the data available from IMDB non-commercial datasets. IMDB provides some data sets although we only use it to retrieve the unique identifiers of the movie that we later use to query data through OMDBs free API. 

- Download flat-file from [IMDB](https://www.imdb.com/interfaces/).
- Process data (e.g. filtering) to deliver unique identifiers.
    - Check filter for movies only
- Write data to Snowflake DB.
- Query daily to get new values.

### OMDB's ratings
Retrieve movie's data from OMDB's api respecting the daily limits, uses IMDB's IDs as input and data is stored in Snowflake.

## Now playing
Retrieve data of what movies are currently playing from local cinema by scrapping the UI of the webpage being mindful of the usage.

- [Pathe now playing](https://en.pathe.nl/films/actueel)

TBD

# Deployment

## Wishlist
- Orchestrate SQL processing using dbt core.
- Orchestrate these two steps using Prefect.
- Compute alternatives: EC2, raspberry pi, Snowflake, codespaces.
- [Implement cost management best practices](https://docs.snowflake.com/en/user-guide/cost-controlling)
- Keep Readme up to date
    - Create diagram of the flow
- Changing schema in OMDB API (check comments in file)
- Scrape data from Pathe: 
- Implement resource monitor in Snowflake
- Unit tests
- Pydantic
- Poetry (package management) 
- Isort
- Black
- pre-commit
- https://github.com/Kanaries/pygwalker
- Check repo best practices here: https://towardsdatascience.com/how-to-structure-an-ml-project-for-reproducibility-and-maintainability-54d5e53b4c82
- Tableau
- Use polars instead of Pandas

## Dependencies
https://github.com/snowflakedb/snowflake-connector-python

## Learnings
- Software Development best practices:
    - Documentation
    - Except type output (check naming)
    - Exception handling
    - Functions
    - Main clause
- Calling an API
- Snowflake