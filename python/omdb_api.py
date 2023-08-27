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
        print('-'*80)
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
        logging.info(f"Successfully retrieved titleids for query: {query}.")
        print('-'*80)
        return sdf
        
    except Exception as e:
        logging.error(f"An error occured while loading data from the db: {str(e)}")


def get_ids_sample(imdb_df, omdb_df) -> list:

    """ Gets a small random sample of titleids by comparing the imdb and omdb dataframes,
    and identifying the titleids not yet present in the omdb df.

    Returns:
        missing_ids_sample: titleids to be queried from OMDB API.
    """

    try:
        missing_ids = imdb_df.exceptAll(omdb_df)
        n_missing_ids = missing_ids.count()
        sample_size = 50
        fraction = sample_size/n_missing_ids
        missing_ids_sample = missing_ids.sample(fraction=fraction).toPandas()
        missing_ids_sample = missing_ids_sample['titleId'].tolist()
        logging.info(f"{missing_ids.count()} rows not found in OMDB RAW Snowflake table.")
        print('-'*80)
        return missing_ids_sample

    except Exception as e:
        logging.error(f"An error occured while loading data from the db: {str(e)}")

def get_omdb_data(titleids_sample) -> pd.DataFrame:

    """ Takes a list of titleids and retrieves the corresponding omdb data.

    Returns:
        omdb_movies: sample df containing movies data such as title, year, imdb rating, etc.
    """

    movies_data = []

    try:

        for id in titleids_sample:
            # Send API request for movie data
            response = requests.get(f'http://www.omdbapi.com/?i={id}&apikey={api_key}')
            
            # Convert API response to dictionary
            r = response.json()
            
            # Add movie data to list
            movies_data.append(r)
            time.sleep(0.1)

        movies_df = pd.DataFrame(movies_data)
        movies_df.columns = movies_df.columns.str.upper()
        movies_df = movies_df[movies_df['TYPE'] == 'movie']
        logging.info(f"Successfully retrieved {len(movies_df)} movies' data from the OMDB API.")
        print('-'*80)

        return movies_df
    
    except Exception as e:
        logging.error(f"An error occured while sending requests to the API: {str(e)}")

def write_movies_data(conn, movies_df) -> None:

    # *: JSON response might vary depending on the API call.
    # If a new column is retrieved an error would be thrown.
    # todo: solve this issue.

    try:
        write_pandas(conn, movies_df, "OMDB_RAW")
        conn.close()
        logging.info('Movie data written to database.')
        print('-'*80)        

    except Exception as e:
        logging.error(f"An error occured writing the data to the db: {str(e)}")    

def main():

    # * Step 1: Create Snowflake connection
    conn = snowflake_connection()
    # * Step 2: Get sample of titleids to be queried from OMDB API.
    imdb_df = retrieve_ids(conn, 'select title_id from omdb.LANDING.imdb_ids')
    omdb_df = retrieve_ids(conn, 'select imdbid from omdb.landing.omdb_raw')
    missing_ids_sample = get_ids_sample(imdb_df, omdb_df)
    # * Step 3: Get movie data from OMDB API and write to database.
    movie_df = get_omdb_data(missing_ids_sample)
    write_movies_data(conn, movie_df)

if __name__ == '__main__':
    begin = dt.now()
    api_key = os.getenv('api_key')
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] %(name)s => %(message)s")
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    main()
    logging.info(
        "Total time - {} minutes".format(round((dt.now()-begin).total_seconds() / 60.0, 2)))