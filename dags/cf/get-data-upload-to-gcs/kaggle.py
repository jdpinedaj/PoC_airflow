import pandas as pd
import numpy as np
import constants
import os
from fastparquet import ParquetFile
from zipfile import ZipFile
from google.cloud import storage

#Connection info
USER = constants.USER  ## CHANGE THIS TO YOUR USERNAME
PASSWORD = constants.PASSWORD  ## CHANGE THIS TO YOUR PASSWORD
PROJECT_ID = constants.PROJECT_ID ## CHANGE THIS TO YOUR PROJECT_ID
GS_BUCKET = constants.GS_BUCKET ## CHANGE THIS TO YOUR GS_BUCKET

def download_data_and_import_file():
    # Download the data from Kaggle
    os.environ['KAGGLE_USERNAME'] = USER # username from the json file
    os.environ['KAGGLE_KEY'] = PASSWORD # key from the json file
    !kaggle datasets download -d usdot/flight-delays --force #TODO: ME PONE PROBLEMA, al parecer no se puede poner un comando bash en cloud function

    # Unzipping and Reading csv files with dtype=string
    zf = ZipFile('flight-delays.zip')
    zf.extractall('') 
    airlines_csv = pd.read_csv('airlines.csv', low_memory=False, dtype=str)
    airports_csv = pd.read_csv('airports.csv', low_memory=False, dtype=str)
    flights_csv = pd.read_csv('flights.csv', low_memory=False, dtype=str)

    # Converting csv to parquet
    airlines_csv.to_parquet("airlines_parquet", compression="GZIP")
    airports_csv.to_parquet("airports_parquet", compression="GZIP")
    flights_csv.to_parquet("flights_parquet", compression="GZIP")

    # Reading parquet files
    airlines_parquet = ParquetFile("airlines_parquet").to_pandas()
    airports_parquet = ParquetFile("airports_parquet").to_pandas()
    flights_parquet = ParquetFile("flights_parquet").to_pandas()

    #Check data
    # flights_parquet.info()

    # Uploading parquet files to Google Cloud Storage
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.get_bucket(GS_BUCKET)
    blob = bucket.blob("airports_parquet.parquet")
    blob.upload_from_filename("airports_parquet.parquet")
    blob = bucket.blob("flights_parquet.parquet")
    blob.upload_from_filename("flights_parquet.parquet")
    blob = bucket.blob("airlines_parquet.parquet")
    blob.upload_from_filename("airlines_parquet.parquet")
