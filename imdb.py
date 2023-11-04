import urllib.request
import gzip
import pandas as pd
import io
import logging
from datetime import date, datetime as dt


import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
import re


def download_file(url, filename) -> pd.DataFrame:

    """
    Takes IMDBs URL that contains a compressed file
    containing titleIDs, returns a Pandas df for further processing.

    Args:
        url (string): URL where the file is located.
        filename: name of the file.

    Returns:
        pandas_df (Pandas dataframe): df containing raw data with movie titles and Ids.

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
        # Create Pandas dataframe
        pandas_df = pd.read_csv('imdb_titles.tsv', sep='\t')
        logging.info("Pandas df successfully created.")    
        return pandas_df                  

    except Exception as e:
        logging.error(f"An error occured extracting the data: {str(e)}")

def data_processing(imdb_raw) -> pd.DataFrame:

    """
    Takes a raw spark df and filters the data to get a smaller set of titleIds.

    Args:
        imdb_raw (Pandas df): raw data containing IMDBs titleIDs and other data.

    Returns:
        clean_imdb_ids (Pandas df): smaller df containing only one column with unique titleIDs.

    """

    try:

        # Filter data on original title, drop remaining duplicates
        logging.info(f"The number of records in the raw table was {imdb_raw.shape[0]}.")
        original_titles = imdb_raw[imdb_raw['isOriginalTitle'] == 1]
        print(original_titles.head())
        logging.info("Filtered original titles only.")
        clean_imdb_ids = original_titles[['titleId', 'title']].drop_duplicates(subset=['titleId']) 
        print(clean_imdb_ids.head())
        logging.info("Dropped duplicates in titleId.")

        # Filter null values in Spark
        clean_imdb_ids = clean_imdb_ids[clean_imdb_ids['titleId'].notna()]
        logging.info("Dropped null values in titleId.")

        # Filter out titleIds that do not follow the pattern
        pattern = '^[a-zA-Z]{2}\d{7,8}$'
        clean_imdb_ids['titleId'] = clean_imdb_ids['titleId'].apply(lambda x: re.match(pattern, x).group(0) if re.match(pattern, x) else None)
        clean_imdb_ids = clean_imdb_ids[clean_imdb_ids['titleId'].notna()]
        clean_imdb_ids.rename(columns={'titleId': 'title_id'}, inplace=True) 

        logging.info("Dropped titleId not matching tt####### pattern.")
        logging.info(f"The number of unique titleIds is {clean_imdb_ids.shape[0]}.")

        return clean_imdb_ids

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

def load_new_data_to_db(conn, df):

    try:
        cur = conn.cursor()
        cur.execute('select * from omdb.LANDING.imdb')
        res = cur.fetchall()
        res_df = pd.DataFrame(res)
        logging.info("Current IMDB Data retrieved from Snowflake.")

        if len(res_df) == 0:
            logging.info("Dataframe in Snowflake is empty, proceeding to load all results.")            
        else:     
            df = df[~df.titleId.isin(res_df.titleId)]
            logging.info(f"Loading {df.shape[0]} records not found in Snowflake table.")
        
    except Exception as e:
        logging.error(f"An error occured while loading data from the db: {str(e)}")

# try:

    if df.shape[0] > 0:

        # Write dataframe to Snowflake table
        print(df.columns)
        write_pandas(conn, df, "IMDB", quote_identifiers=False)
        conn.close()
        logging.info('TitleIds written to snowflake db successfully.')
    else:
        pass
        logging.info('Now rows to be added, loading to snowflake not needed.')

# except Exception as e:
#     logging.error(f"An error occured loading the data to the db: {str(e)}")


def main():

    raw_df = download_file(url, filename)
    print(raw_df.head())
    imdb_ids = data_processing(raw_df)
    print(imdb_ids.head())
    conn = snowflake_connection()
    load_new_data_to_db(conn, imdb_ids)


if __name__ == '__main__':
    begin = dt.now()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] %(name)s => %(message)s")
    url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
    filename = 'title.akas.tsv.gz'
    main()
    logging.info(
        "Total time - {} minutes".format(round((dt.now()-begin).total_seconds() / 60.0, 2)))    