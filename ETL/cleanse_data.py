import logging
import sys
import os
import re
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from operator import add
from functools import reduce

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
    IMSS_files_location = os.path.join('IMSS_Files')
    logger.debug(f'Looking for files in {IMSS_files_location}')

    # Get list of files in target location
    IMSS_files = os.listdir(IMSS_files_location)
    logger.debug(f'Retrieved the following file list {IMSS_files}')

    # Concatenate full filepath
    IMSS_path_list = [os.path.join('IMSS_Files',file) for file in IMSS_files]
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
    # Create month number to name dictionary:
    month_dict = {
        '01' : 'Enero', '02' : 'Febrero', '03' : 'Marzo',
        '04' : 'Abril', '05' : 'Mayo', '06' : 'Junio',
        '07' : 'Julio', '08' : 'Agosto', '09' : 'Septiembre',
        '10' : 'Octubre', '11' : 'Noviembre', '12' : 'Diciembre'
    }

    # Create pattern to extract the filename
    pattern = r'(?<=\\)[^\\]+\.csv$'
    # Apply regex pattern
    match = re.search(pattern, filepath)
    # Get filename
    filename = match.group()
    logger.debug(f'Extracted {filename} from {filepath}')

    # Get file month 
    month = month_dict[re.search(r'-\d{2}-', filename).group(0).replace('-','')]
    # Get file year
    year = re.search(r'\d{4}', filename).group(0)

    logger.debug(f'Data was extracted from month {month} for year {year}')

    return month, year

def filter_by_state(df, state):
    filtered_df = df.filter(df.cve_entidad == state)
    return filtered_df

def non_contextual_transformations(df):
    # Drop entries where there is no salary information
    no_nulls_df = df.dropna(subset=['sector_economico_1', 'sector_economico_2', 'sector_economico_4'])
    # Count how many rows there were before filtering
    rows_in_source_df = df.count()
    # Count how many rows are left
    rows_in_filtered_df = no_nulls_df.count()
    # Calculate the rows that were removed
    removed_entries = rows_in_source_df - rows_in_filtered_df
    logger.warning(f'Removed {removed_entries} NaN entries from the dataframe')
    # Drop entries where complete duplicates are found
    transformed_df = no_nulls_df.dropDuplicates()
    # Count how many rows are left after removing duplicates
    removed_duplicate_count = transformed_df.count()

    logger.warning(f'Removed {rows_in_filtered_df - removed_duplicate_count} duplicate entries from dataframe')
    logger.debug(f'Source dataframe originally had {rows_in_source_df} rows')
    logger.debug(f'Clean dataframe now has {removed_duplicate_count} rows')

    # Count of remaining nulls and NaNs per column:
    
    null_counts = transformed_df.select([f.count(f.when(f.isnull(c), c)).alias(f'{c}_nan_count')  
                                        for c in transformed_df.columns])
    nan_counts = transformed_df.select([f.count(f.when(f.isnan(c), c)).alias(f'{c}_nan_count')  
                                        for c in transformed_df.columns])
    summed_null_count = null_counts.withColumn('total',reduce(add, [f.col(x) for x in null_counts.columns])).select('total')
    summed_nan_count = nan_counts.withColumn('total',reduce(add, [f.col(x) for x in nan_counts.columns])).select('total')
    logger.info(f'NaN count in each column: {nan_counts.take(1)}')
    logger.info(f'Null count in each columns: {null_counts.take(1)}')
    logger.info(f'Total null count {summed_null_count.take(1)}')
    logger.info(f'Total nan count {summed_nan_count.take(1)}')
    
    return transformed_df
    
def cast_dtypes(df):
    # Import the dtype dictionary 
    IMSS_files_dtypes = {"cve_delegacion": ByteType(), "cve_subdelegacion": ByteType(), 
                        "cve_entidad": ByteType(), "cve_municipio": StringType(), 
                        "sector_economico_1": ShortType(), "sector_economico_2": ShortType(), 
                        "sector_economico_4": ShortType(), "tamaño_patron": StringType(), 
                        "sexo": ByteType(), "rango_edad": StringType(), 
                        "rango_salarial": StringType(), "rango_uma": StringType(), 
                        "asegurados": IntegerType(), "no_trabajadores": IntegerType(), 
                        "ta": ShortType(), "teu": ShortType(), "tec": ShortType(), 
                        "tpu": ShortType(), "tpc": ShortType(), "ta_sal": ShortType(), 
                        "teu_sal": ShortType(), "tec_sal": ShortType(), "tpu_sal": ShortType(), 
                        "tpc_sal": ShortType(), "masa_sal_ta": FloatType(), "masa_sal_teu": FloatType(), 
                        "masa_sal_tec": FloatType(), "masa_sal_tpu": FloatType(), "masa_sal_tpc": FloatType()}

    # Cast dataframe as the correct dtype
    for name, dtype in IMSS_files_dtypes.items():
        if name in df.columns:
            logger.debug(f'Casting {name} into {dtype}')
            df = df.withColumn(name, f.col(name).cast(dtype))
        else:
            logger.warning(f'{name} does not exist in dataframe')
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
        csv_month, csv_year = IMSS_filename_parser(file)

        logger.debug(f'Opening {file} to read as a DataFrame')
        df = read_files(spark,file)

        logger.debug(f'Selecting rows where state matches {state}')
        filtered_df = filter_by_state(df, state)
        
        logger.debug(f'Removing NaN and duplicate values and changing dtypes to the most efficient ones')
        clean_df = cast_dtypes(non_contextual_transformations(filtered_df))

        clean_df = clean_df.withColumn('month', f.lit(csv_month))
        clean_df = clean_df.withColumn('year', f.lit(csv_year))

        logger.debug(f'Final DataFrame Sample: {clean_df.take(1)}')

        logger.info(f'Removing {file}')
        # os.remove(file)


    logger.debug(f'Finished cleaning csv files in {csv_file_paths}')

    spark.stop()

if __name__ == "__main__":
    main()