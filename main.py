import logging
from pyspark.sql.window import Window
from pyspark.sql.functions import mean, when, stddev, col, min, max, lag, lead, coalesce, abs, lit
from pyspark.sql import SparkSession, dataframe
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

# TODO: Write tests

# Setup logging
# Reason: To keep track of the data processing
def setup_logging():
    logging.basicConfig(
        filename='data_pipeline.log',
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


# Get the last uploaded dates for each turbine
# Assumption: New data continues from the day after the last uploaded date
def get_last_times() -> datetime or None:
    url = f"jdbc:postgresql://{DB_HOST}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    query = "(SELECT MAX(timestamp) AS max_time FROM turbine_data_raw) AS last_time"
    try:
        last_time = spark.read.jdbc(url=url, table=query, properties=properties)
    except Exception as e:
        logger.error(f"Error getting last upload time: {e}")
        return None
    return last_time.collect()[0][0]


# Query the database for all the data in the last n hours from a table
def get_data_from_sql(table_name: str, start_time: datetime) -> dataframe.DataFrame:
    url = f"jdbc:postgresql://{DB_HOST}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    query = f"(SELECT * FROM {table_name} WHERE timestamp > '{start_time}') AS last_n_hours"
    try:
        df = spark.read.jdbc(url=url, table=query, properties=properties)
        logger.info(f"Data gotten from {table_name}")
    except Exception as e:
        logger.error(f"Error getting data from {table_name}: {e}")
        return None
    return df


# Replace null values with the mean of the column
def fill_nas_with_mean(df: dataframe.DataFrame, colName: str) -> dataframe.DataFrame:
    windowSpec = Window.partitionBy('turbine_id').orderBy('timestamp')
    df = df.withColumn('mean', mean(col(colName)).over(windowSpec))
    df = df.withColumn(colName, coalesce(col(colName), col('mean')))
    df = df.drop('mean')
    return df


# Remove duplicate rows based on the given columns
def remove_duplicate_rows(df: dataframe.DataFrame, columns: str or list) -> dataframe.DataFrame:
    df = df.dropDuplicates(columns)
    return df


# Replace outliers with the mean of the column
def replace_outliers_with_mean(df: dataframe.DataFrame, colName: str) -> dataframe.DataFrame:
    windowSpec = Window.partitionBy('turbine_id').orderBy('timestamp')
    df = df.withColumn('mean', mean(colName).over(windowSpec))
    df = df.withColumn('stddev', stddev(colName).over(windowSpec))
    df = df.withColumn(colName, when(
        abs(df[colName] - df['mean']) > 2 * df['stddev'],
        df['mean']
    ).otherwise(df[colName]))
    df = df.drop('mean', 'stddev')
    return df


# Calculate the mean, min and max values of each turbine
def calculate_stats(df: dataframe.DataFrame, colName: str, start_time: datetime, end_time: datetime) -> dataframe.DataFrame:
    df = df.groupBy('turbine_id').agg(
        mean(col(colName)).alias('avg_'+colName),
        min(col(colName)).alias('min_'+colName),
        max(col(colName)).alias('max_'+colName)
    )
    df = df.withColumn('timestamp_from', lit(start_time))
    df = df.withColumn('timestamp_to', lit(end_time))
    return df


# Detect anomalies using the 2-sigma rule
def detect_anomalies(df: dataframe.DataFrame, colName: str, n_days : int) -> dataframe.DataFrame:
    windowSpec = Window.partitionBy('turbine_id')
    df = df.withColumn('mean_'+colName, mean(col(colName)).over(windowSpec)) \
        .withColumn('stddev_'+colName, stddev(col(colName)).over(windowSpec))
    df = df.withColumn('lower_bound_'+colName, col('mean_'+colName) - 2 * col('stddev_'+colName)) \
        .withColumn('upper_bound_'+colName, col('mean_'+colName) + 2 * col('stddev_'+colName))
    df = df.filter((col(colName) < col('lower_bound_'+colName)) | (col(colName) > col('upper_bound_'+colName)))
    df = df.withColumn('calc_days', lit(n_days))
    df = df.drop('mean_'+colName, 'stddev_'+colName, 'wind_speed', 'wind_direction')
    return df


# Filter out the data that has already been uploaded
def filter_for_new_data(df: dataframe.DataFrame, last_upload_times: None or datetime) -> dataframe.DataFrame:
    df_filtered = df if last_upload_times == None else df.filter(df.timestamp > last_upload_times)
    return df_filtered


# Upload the data to the database
def upload_data_to_sql(df: dataframe.DataFrame, table_name: str) -> bool:
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}/{DB_NAME}") \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .save(mode='append')
        logger.info(f"{df.count()} rows uploaded to {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading data to {table_name}: {e}")
        return False


# Clear the csv files of processed data
'''
Reason: The processed data is already in the database, so it is not needed in the csv files
and it is good to keep the csv files as small as possible to improve read operations at the begginning of the pipeline
'''
def clear_csv(df: dataframe.DataFrame, file_name: str) -> None:
    df = df.filter(df.turbine_id == 0)
    df = df.toPandas()
    try:
        df.to_csv(file_name, index=False)
        logger.info(f"{file_name} file cleared")
    except Exception as e:
        logger.error(f"Error writing data to {file_name}: {e}")
    

# Check stats exists in database
def check_stats_exists_in_db(start_time: datetime, end_time: datetime) -> bool:
    url = f"jdbc:postgresql://{DB_HOST}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    query = f"(SELECT COUNT(*) FROM summary_statistics WHERE timestamp_from = '{start_time}' AND timestamp_to = '{end_time}') AS stats_exists"
    try:
        stats_exists = spark.read.jdbc(url=url, table=query, properties=properties)
        logger.info(f"Checked if stats exists in database")
    except Exception as e:
        logger.error(f"Error checking if stats exists in database: {e}")
        return False
    
    return True if stats_exists.collect()[0][0] > 0 else False


# Clean data
def clean_data(df: dataframe.DataFrame) -> dataframe.DataFrame:
    columns = ['wind_speed', 'wind_direction', 'power_output']
    for colName in columns:
        df = fill_nas_with_mean(df, colName)
        df = df if colName == 'power_output' else replace_outliers_with_mean(df, colName)
    return df


if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()
    
    logger.info(f"\n{'_'*60}")
    logger.info('Starting data pipeline')

    sdf1_path = './raw_data/data_group_1.csv'
    sdf2_path = './raw_data/data_group_2.csv'
    sdf3_path = './raw_data/data_group_3.csv'
    
    # Initialize Spark Session
    spark = SparkSession.builder.master('local').appName("WindTurbineDataPipeline") \
    .config("spark.jars", "file:///c:/project/pyspark/jdbc_driver/postgresql-42.7.1.jar") \
    .config('spark.driver.extraClassPath', 'file:///c:/project/pyspark/jdbc_driver/postgresql-42.7.1.jar') \
    .getOrCreate()
    logger.info('Spark Session initialized')
    spark.sparkContext.setLogLevel("ERROR")

    # Set the number of hours to filter for
    # In this case, it is set to 30 days
    N_HOURS = 720
    logger.info(f'Number of hours to filter for: {N_HOURS}')

    # Get the last uploaded time
    last_uploaded_time = get_last_times()
    logger.info(f'Last upload time: {last_uploaded_time} gotten')
    
    # Read the CSV files into a DataFrames
    sdf1 = spark.read.csv(sdf1_path, header=True, inferSchema=True)
    sdf2 = spark.read.csv(sdf2_path, header=True, inferSchema=True)
    sdf3 = spark.read.csv(sdf3_path, header=True, inferSchema=True)
    logger.info('CSV files read into DataFrames')

    # Join the DataFrames
    sdf = sdf1.union(sdf2).union(sdf3)
    logger.info('DataFrames joined')

    # Set dataframe data types
    sdf = sdf.withColumn('timestamp', sdf['timestamp'].cast('timestamp'))
    sdf = sdf.withColumn('turbine_id', sdf['turbine_id'].cast('int'))
    sdf = sdf.withColumn('wind_speed', sdf['wind_speed'].cast('float'))
    sdf = sdf.withColumn('wind_direction', sdf['wind_direction'].cast('float'))
    sdf = sdf.withColumn('power_output', sdf['power_output'].cast('float'))
    logger.info('Data types set')

    # Filter for new data
    sdf = filter_for_new_data(sdf, last_uploaded_time)

    # Get the latest time from csv files
    latest_csv_time = sdf.agg(max('timestamp')).collect()[0][0]
    logger.info(f'Latest time from csv files: {latest_csv_time}')

    # Get the end time for statistics and anomalies
    end_time = latest_csv_time if latest_csv_time != None else last_uploaded_time
    logger.info(f'End time for statistics and anomalies: {end_time}')

    # Get the start time for statistics and anomalies
    start_time = end_time - timedelta(hours=N_HOURS)
    logger.info(f'Start time for statistics and anomalies: {start_time}')

    # Get the data from the database
    sdf_db = get_data_from_sql('turbine_data_raw', start_time)
    logger.info('Data from database gotten')

    # Remove rows where timestamp is null
    sdf = sdf.filter(sdf.timestamp.isNotNull())
    logger.info('Rows with null timestamp removed')

    # Remove duplicate rows
    sdf = remove_duplicate_rows(sdf, ['timestamp', 'turbine_id'])
    logger.info('Duplicate rows removed')

    # Clean the data
    clean_df = clean_data(sdf)
    logger.info('Data cleaned')

    # Join the data from the database with the new data
    joined_sdf = clean_df.union(sdf_db) if sdf_db != None else clean_df

    # Calculate the statistics for each turbine
    check_stats_in_db = check_stats_exists_in_db(start_time, end_time)
    stats_df = calculate_stats(joined_sdf.select('turbine_id', 'power_output'), 'power_output', start_time, end_time) if check_stats_in_db == False else None
    logger.info('Statistics calculated' if stats_df != None else 'Statistics calculation skipped')

    # Calculate the anomalies for each turbine
    anomalies_df = detect_anomalies(joined_sdf, 'power_output', N_HOURS/24)
    logger.info(f"{'No a' if anomalies_df.count() == 0 else 'A'}nomalies detected")
    
    # Upload the raw and processed dataframes to the database
    raw_data_upload = upload_data_to_sql(sdf, 'turbine_data_raw') if sdf.count() > 0 else False
    clean_data_upload = upload_data_to_sql(clean_df, 'turbine_data_cleaned') if clean_df.count() > 0 else False
    logger.info(f'Data upload to database section {"completed" if (raw_data_upload or clean_data_upload) else "skipped"}')

    # Upload the daily statistics and anomalies to the database
    stat_upload = upload_data_to_sql(stats_df, 'summary_statistics') if check_stats_in_db == False else False
    logger.info(f"Daily statistics upload section {'complete' if stat_upload else 'skipped'}")

    # Upload the anomalies to the database
    anomalies_upload = upload_data_to_sql(anomalies_df, 'anomalies') if anomalies_df.count() > 0 else False
    logger.info(f"Anomalies upload section {'complete' if anomalies_upload else 'skipped'}")

    # Write the filtered data to csv files
    if raw_data_upload or clean_data_upload:
        clear_csv(sdf1, sdf1_path)
        clear_csv(sdf2, sdf2_path)
        clear_csv(sdf3, sdf3_path)
        logger.info('Filtered data written to csv files')

    # Stop Spark Session
    spark.stop()
    logger.info('Spark Session stopped')