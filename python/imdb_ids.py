import urllib.request
import gzip
import pandas as pd
import io
import logging
from datetime import date, datetime as dt
from pyspark.sql import SparkSession
import pyspark.sql.utils
from pyspark.sql.functions import col, regexp_extract, when
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

# Configuration

# Code Description ......

def download_file(url, filename) -> "pyspark.sql.DataFrame":

    """
    Takes IMDBs URL that contains a compressed file
    containing titleIDs, returns a Spark df for further processing.

    Args:
        url (string): URL where the file is located.
        filename: name of the file.

    Returns:
        spark_df (Spark dataframe): df containing raw data with movie titles and Ids.

    """

    try:
        # Download and extract the file
        urllib.request.urlretrieve(url, filename)
        logging.info("File retrieved from URL.")
        with gzip.open(filename, 'rb') as f:
            file_content = f.read()
        logging.info("File opened from compressed file.")
        # Save the contents to a local file
        with open('imdb_titles.tsv', 'wb') as f:
            f.write(file_content)
        logging.info(".tsv file downloaded.")
        # Create Spark dataframe
        spark_df = spark.read.csv('imdb_titles.tsv', header=True, sep='\t')
        logging.info("Spark df successfully created.")    
        return spark_df                  

    except Exception as e:
        logging.error(f"An error occured extracting the data: {str(e)}")

def spark_processing(spark_df) -> "pyspark.sql.DataFrame":

    """
    Takes a raw spark df and filters the datato get a smaller set of titleIds.

    Args:
        df (Spark dataframe): raw data containing IMDBs titleIDs and other data.

    Returns:
        spark_filtered_df (Spark dataframe): smaller df containing only one column with unique titleIDs.

    """

    try:

        # Filter data on original title, drop remaining duplicates
        logging.info(f"The number of records in the raw table was {spark_df.count()}.")
        df_original = spark_df.filter(spark_df.isOriginalTitle == 1)
        logging.info("Filtered original titles only.")
        spark_imdb_ids = df_original.select("titleId").dropDuplicates()
        logging.info("Dropped duplicates in titleId.")

        # Filter null values in Spark
        spark_imdb_ids = spark_imdb_ids.filter(spark_imdb_ids.titleId.isNotNull())
        logging.info("Dropped null values in titleId.")

        # Filter out titleIds that do not follow the pattern
        pattern = '^[a-zA-Z]{2}\d{7,8}$' 
        spark_filtered_df = spark_imdb_ids.withColumn('titleId', regexp_extract(spark_imdb_ids.titleId, pattern, 0))
        spark_filtered_df = spark_imdb_ids.withColumn('titleId', when(spark_imdb_ids.titleId == '', None).otherwise(spark_imdb_ids.titleId))
        spark_filtered_df = spark_imdb_ids.filter(spark_imdb_ids.titleId.isNotNull())
        logging.info("Dropped titleId not matching tt####### pattern.")
        logging.info(f"The number of unique titleIds is {spark_filtered_df.count()}.")

        return spark_filtered_df

    except Exception as e:
        logging.error(f"An error transforming the data: {str(e)}")

def snowflake_connection() -> snowflake.connector.connection:

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

def get_new_data_only(conn, spark_df):

    try:
        cur = conn.cursor()
        cur.execute('select * from omdb.LANDING.imdb_ids')
        logging.info("Cursor executed")
        rows = cur.fetchall()
        logging.info("Fetchall done.")

        if len(rows) == 0:
            logging.info("Dataframe in Snowflake is empty, returning all results.")
            return spark_df
        else:     
            sf = spark.createDataFrame(rows, schema=['titleId'])
            new_rows = spark_df.exceptAll(sf)
            logging.info(f"{new_rows.count()} rows not found in Snowflake table.")
            return new_rows
        
    except Exception as e:
        logging.error(f"An error occured while loading data from the db: {str(e)}")


def load_to_snowflake(conn, spark_df) -> None:


    try:

        if spark_df.count() > 0:
            # Convert Spark DataFrame to Pandas DataFrame
            pandas_imdb_ids = spark_df.select("*").toPandas()
            pandas_imdb_ids.rename(columns={'titleId': 'TITLE_ID'}, inplace=True)
            pandas_imdb_ids.sort_values("TITLE_ID", ascending=True)
            logging.info("Spark df converted to Pandas df")

            # Write dataframe to Snowflake table
            write_pandas(conn, pandas_imdb_ids, "IMDB_IDS")
            conn.close()
            logging.info('TitleIds written to snowflake db successfully.')
        else:
            pass
            logging.info('Now rows to be added, loading to snowflake not needed.')

    except Exception as e:
        logging.error(f"An error occured loading the data to the db: {str(e)}")


def main():

    spark_raw_df = download_file(url, filename)
    spark_reduced_df = spark_processing(spark_raw_df)
    conn = snowflake_connection()
    new_rows = get_new_data_only(conn, spark_reduced_df)
    load_to_snowflake(conn, new_rows)


if __name__ == '__main__':
    begin = dt.now()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] %(name)s => %(message)s")
    url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
    filename = 'title.akas.tsv.gz'
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    main()
    logging.info(
        "Total time - {} minutes".format(round((dt.now()-begin).total_seconds() / 60.0, 2)))    