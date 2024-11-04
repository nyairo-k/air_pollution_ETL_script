#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import requests
import pandas as pd
import sqlalchemy
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

def data_collection():
    try:
        current_unix_time = int(time.time())

        # API endpoint for fetching and parsing data
        hist_url = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat=-1.4481149&lon={36.971587}&start=1609459200&end={current_unix_time}&appid=0dd6916655bc878d88bc6ea58e9889a8'

        data = requests.get(hist_url)

        # Check if the request was successful
        if data.status_code != 200:
            logging.error(f"Failed to fetch data: {data.status_code}")
            return None  # Exit the function if the request fails

        jdata = data.json()

        # Gather key-value pairs for my DataFrame and append to this list
        data_list = []

        # Identify key to work with
        key = jdata['list']

        # Loop through entries in the key
        for entry in key:
            dt = entry['dt']  # Get the Unix timestamp
            aqi = entry['main']['aqi']
            components = entry['components']

            # Create a new dictionary for the values I want
            new_dict = {'dt': dt, 'aqi': aqi, **components}  # Unpack the nested dictionary
            data_list.append(new_dict)

        # Create a DataFrame from the collected data
        df = pd.DataFrame(data_list)

        # Convert the 'dt' column from Unix timestamps to human-readable datetime format
        df['dt'] = pd.to_datetime(df['dt'], unit='s')

        # Remove duplicates based on the 'dt' column
        df = df.drop_duplicates(subset='dt')
        logging.info(f"Removed duplicates. DataFrame shape: {df.shape}")

        # Database connection parameters
        db_params = {
            'dbname': 'Air Pollution Athi River',
            'user': 'postgres',
            'password': '123Sheba',
            'host': 'localhost',
            'port': '5432'
        }

        # Create the SQLAlchemy engine
        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}')

        # Load existing 'dt' values from the database to avoid duplicates
        existing_df = pd.read_sql('SELECT dt FROM air_data', engine)
        existing_dts = existing_df['dt'].tolist()

        # Filter out the rows in df that already exist in the database
        new_df = df[~df['dt'].isin(existing_dts)]
        logging.info(f"DataFrame after filtering existing records: {new_df.shape}")

        if not new_df.empty:
            # Push new data to the database only if there are new records
            new_df.to_sql('air_data', engine, if_exists='append', index=False)
            logging.info("Data inserted successfully into the database.")
        else:
            logging.info("No new data to insert into the database.")

        # Instead of returning only new data, return all data from the database
        all_data_df = pd.read_sql('SELECT * FROM air_data', engine)
        return all_data_df

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None  # Return None if there's an error

# Example usage
if __name__ == "__main__":
    result_df = data_collection()
    if result_df is not None:
        print(result_df.info())
    else:
        print("No data collected.")


# In[2]:


import schedule 
schedule.every().hour.do(data_collection)


# In[ ]:


#create an infinite loop to ensure the function runs continuously 
while True:
    schedule.run_pending()
    time.sleep(60)


# In[ ]:




