# omdb

# Steps
- 1. Get titleids from IMDB, write new data to Snowflake DB.
https://www.imdb.com/interfaces/
- 2. Use titleids to extract movie information from OMDB API.
- 3. Check how to orchestrate these two steps (Airflow, Prefect).
- 4. Check how to put this code into production (check cloud solutions: gcp, codespaces?)
- 5. Transform the data with dbt, integrate to flow.

# Visual Studio
- How to use the same way as Pycharm?
- How to debug?
- What are the best plugins?

# Todo:
- Spark fixes:
    - Revisar WARN TaskSetManager: stage contains a task of very large size
    - WARN GarbageCollectionMetrics
    - You have an incompatible version of 'pyarrow' installed (11.0.0), please install a version that adheres to: 'pyarrow<10.1.0,>=10.0.1; extra == "pandas"'
    - WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
- Check if I can filter for movies only
- Write documentation (Readme)
    - Explain project process
    - Explain dependencies

# Tools & frameworks to explore
- Unit tests
- Pydantic
- Poetry (package management) 
- Isort
- Black
- pre-commit
- https://github.com/Kanaries/pygwalker
- Check repo best practices here: https://towardsdatascience.com/how-to-structure-an-ml-project-for-reproducibility-and-maintainability-54d5e53b4c82
- Tableau

# DB Optimization
- Change varchar type
- Check cost cap in Snowflake
https://docs.snowflake.com/en/user-guide/cost-controlling

# What I have learnt
- Software Development best practices:
    - Documentation
    - Except type output (check naming)
    - Exception handling
    - Functions
    - Main clause
- Spark
- Calling an API
- Snowflake