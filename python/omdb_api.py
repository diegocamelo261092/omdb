import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
from datetime import date, datetime as dt
import logging
from pyspark.sql import SparkSession
import pyspark.sql.utils
import time

# todo: add overall description here


def snowflake_connection() -> snowflake.connector.connection:

    """ Creates a connection to Snowflake.
    Gets credentials from .env file.

    Returns:
        conn: Snowflake connection object.
    """

    try:
        # Snowflake configuration and connection
        conn = snowflake.connector.connect(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account=os.getenv('account'),
            warehouse=os.getenv('warehouse'),
            database=os.getenv('database'),
            schema=os.getenv('schema')
            )
        logging.info("Successfully connected to Snowflake.")
        return conn

    except Exception as e:
        logging.error(f"An error occured connecting to the db: {str(e)}")

def retrieve_ids(conn, query) -> pyspark.sql.DataFrame:

    """ Retrieves titleids from the Snowflake database.

    Returns:
        sdf: spark dataframe with titleids.
    """

    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        sdf = spark.createDataFrame(rows, schema=['titleId'])
        return sdf
        
    except Exception as e:
        logging.error(f"An error occured while loading data from the db: {str(e)}")


def get_ids_sample(imdb_df, omdb_df) -> list:

    """ Gets a small random sample of titleids by comparing the imdb and omdb dataframes,
    and identifying the ones that are not in both.

    Returns:
        missing_ids_sample: titleids to be queried from OMDB API.
    """

    try:
        missing_ids = imdb_df.exceptAll(omdb_df)
        n_missing_ids = missing_ids.count()
        sample_size = 50
        fraction = sample_size/n_missing_ids
        print(fraction)
        missing_ids_sample = missing_ids.sample(fraction=fraction).toPandas()
        missing_ids_sample = missing_ids_sample['titleId'].tolist()
        logging.info(f"{missing_ids.count()} rows not found in Snowflake table.")
        return missing_ids_sample
    except Exception as e:
        logging.error(f"An error occured while loading data from the db: {str(e)}")

def populate_omdb_table(titleids_sample) -> pd.DataFrame:

    data = [] # List to store retrieved data

    for id in titleids_sample:
        # Send API request for movie data
        response = requests.get(f'http://www.omdbapi.com/?i={id}&apikey={api_key}')
        
        # Convert API response to dictionary
        movie_data = response.json()
        
        # Add movie data to list
        data.append(movie_data)
        time.sleep(0.5)
        print(id)

    # Convert data list to DataFrame
    omdb_movies = pd.DataFrame(data)

    # capitalize all column names
    omdb_movies.columns = omdb_movies.columns.str.upper()

    return omdb_movies

def main():
    conn = snowflake_connection()
    imdb_df = retrieve_ids(conn, 'select title_id from omdb.LANDING.imdb_ids')
    omdb_df = retrieve_ids(conn, 'select imdbid from omdb.landing.omdb_raw')
    missing_ids_sample = get_ids_sample(imdb_df, omdb_df)
    final_df = populate_omdb_table(missing_ids_sample)
    print(final_df.head(10))

if __name__ == '__main__':
    begin = dt.now()
    api_key = os.getenv('api_key')
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] %(name)s => %(message)s")
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    main()
    logging.info(
        "Total time - {} minutes".format(round((dt.now()-begin).total_seconds() / 60.0, 2)))