import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

api_key = '3caf96d9' # Replace with your OMDB API key
ids = ['tt0088163', 'tt1430116', 'tt1560685'] # List of movie titles to retrieve data for

data = [] # List to store retrieved data

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


for id in ids:
    # Send API request for movie data
    response = requests.get(f'http://www.omdbapi.com/?i={id}&apikey={api_key}')
    
    # Convert API response to dictionary
    movie_data = response.json()
    
    # Add movie data to list
    data.append(movie_data)

# Convert data list to DataFrame
df = pd.DataFrame(data)

# capitalize all column names
df.columns = df.columns.str.upper()

print(df)
print(df.info())
write_pandas(conn, df, "OMDB_RAW")
conn.close()
print('done')