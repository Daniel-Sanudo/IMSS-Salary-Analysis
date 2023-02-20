import logging
import sys
import os
import json
import re
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create logger basic config
logging.basicConfig(handlers=[logging.FileHandler(filename="cleanse_data.log",
                                                 encoding='windows-1252', mode='w')],
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

# Create logger object that will be used to debug the app
logger = logging.getLogger(__name__)

# Create pyspark log to avoid cramming the debug log with pyspark messages
pyspark_log = logging.getLogger('pyspark').setLevel(logging.ERROR)

# Create pyspark log to avoid cramming the debug log with py4j messages
py4j_logger = logging.getLogger("py4j").setLevel(logging.ERROR)

def get_file_path():
    # Use getcwd to get the current folder location, then change the final folder in path to access the Scraping folder
    IMSS_files_location = os.path.join('..','Scraping','IMSS_Files')
    logger.debug(f'Looking for files in {IMSS_files_location}')

    # Get list of files in target location
    IMSS_files = os.listdir(IMSS_files_location)
    logger.debug(f'Retrieved the following file list {IMSS_files}')

    # Concatenate full filepath
    IMSS_path_list = [os.path.join('..','Scraping','IMSS_Files',file) for file in IMSS_files]
    logger.debug(f'Filepath list: {IMSS_path_list}')

    return IMSS_path_list

def read_files(spark, csv_path):
    # Define the reading options
    sep = '|'
    encoding = 'windows-1252'

    # Read the csv file using the defined reading options
    df = spark.read.load(csv_path,
                     format="csv", sep=sep, inferSchema="false", header="true", encoding=encoding)

    logger.debug(f'Read dataframe with the following columns and dtypes {df.dtypes}')

    return df

def IMSS_filename_parser(filepath):
    pattern = r"(?<=\\)[^\\]+\.csv$"
    match = re.search(pattern, filepath)
    filename = match.group()
    logger.debug(f'Extracted {filename} from {filepath}')
    return filename

def filter_by_state(df, state):
    filtered_df = df.filter(df.cve_entidad == state)
    return filtered_df

def non_contextual_transformations(df):
    no_nulls_df = df.dropna(subset=['sector_economico_1', 'sector_economico_2', 'sector_economico_4'])
    rows_in_source_df = df.count()
    rows_in_filtered_df = no_nulls_df.count()
    removed_entries = rows_in_source_df - rows_in_filtered_df
    logger.warning(f'Removed {removed_entries} NaN entries from the dataframe')
    logger.debug(f'Source dataframe originally had {rows_in_filtered_df} rows')
    

def cast_dtypes(df):
    # Import the dtype dictionary 
    with open('PySpark_IMSS_files_dtypes.json') as dtypes_json:
        IMSS_files_dtypes = json.load(dtypes_json)
    logger.debug(f'Using the following schema: \n {IMSS_files_dtypes}')

    # Cast dataframe as the correct dtype
    for name, dtype in IMSS_files_dtypes.items():
        logger.debug(f'Casting {name} into {dtype}')
        df = df.withColumn(name, f.col(name).cast(dtype))

    return df



def main():
    # Create Spark Session
    spark = SparkSession.builder \
        .appName('ETL') \
        .master('local[*]') \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('OFF')

    csv_file_paths = get_file_path()

    state = sys.argv[1]

    for file in csv_file_paths:
        csv_date = IMSS_filename_parser(file)

        logger.debug(f'Opening {file} to read as a DataFrame')
        df = read_files(spark,file)

        logger.debug(f'Selecting rows where state matches {state}')
        filtered_df = filter_by_state(df, state)
        
        clean_df = non_contextual_transformations(filtered_df)

    logger.debug

    spark.stop()

if __name__ == "__main__":
    main()