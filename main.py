import logging
from pyspark.sql.window import Window
from pyspark.sql.functions import mean, when, stddev, col, min, max, lag, lead, coalesce, to_timestamp, abs
from pyspark.sql import SparkSession, dataframe
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

# TODO: Possible implementation of last upload time for each turbine
# TODO: Implement a filter of csv files for the last n days
# TODO: Implement a scalable version of anomaly detection that filters for the last n days and uses that to detect anomalies
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


# Calculate the mean of the nearest non-null values
'''
Reason: The data is sampled every 1 hour, so the nearest non-null values are likely 
to be similar and it is good to keep as many data points as possible for analysis
'''
def fill_nas_with_mean(df: dataframe.DataFrame, colName: str) -> dataframe.DataFrame:
    windowSpec = Window.partitionBy('turbine_id').orderBy('timestamp')
    df = df.withColumn('prev_value', lag(colName, 1).over(windowSpec))
    df = df.withColumn('next_value', lead(colName, 1).over(windowSpec))
    df = df.withColumn('mean_nearest', (col('prev_value') + col('next_value'))/2)
    df = df.withColumn(colName, coalesce(col(colName), col('mean_nearest')))
    df = df.drop('prev_value', 'next_value', 'mean_nearest')
    return df


# Remove duplicate rows based on the given columns
def remove_duplicate_rows(df: dataframe.DataFrame, columns: str or list) -> dataframe.DataFrame:
    df = df.dropDuplicates(columns)
    return df


# Replace outliers with the mean of the nearest non-null values
def replace_outliers_with_mean(df: dataframe.DataFrame, colName: str) -> dataframe.DataFrame:
    windowSpec = Window.partitionBy('turbine_id', 'date').orderBy('timestamp')
    
    df = df.withColumn('mean', mean('power_output').over(windowSpec))
    df = df.withColumn('stddev', stddev('power_output').over(windowSpec))
    df = df.withColumn('power_output', when(
        abs(df['power_output'] - df['mean']) > 2 * df['stddev'],
        df['mean']
    ).otherwise(df['power_output']))
    df = df.drop('mean', 'stddev')
    return df


# Calculate the mean, min and max values of each turbine for each day
def calculate_daily_stats(df: dataframe.DataFrame, colName: str) -> dataframe.DataFrame:
    df = df.groupBy('turbine_id', 'date').agg(
        mean(col(colName)).alias('avg_'+colName),
        min(col(colName)).alias('min_'+colName),
        max(col(colName)).alias('max_'+colName)
    )
    return df


# Detect anomalies using the 2-sigma rule
def detect_anomalies(df: dataframe.DataFrame, colName: str) -> dataframe.DataFrame:
    windowSpec = Window.partitionBy('time', 'turbine_id')
    df = df.withColumn('mean_'+colName, mean(col(colName)).over(windowSpec)) \
        .withColumn('stddev_'+colName, stddev(col(colName)).over(windowSpec))
    df = df.withColumn('lower_bound_'+colName, col('mean_'+colName) - 2 * col('stddev_'+colName)) \
        .withColumn('upper_bound_'+colName, col('mean_'+colName) + 2 * col('stddev_'+colName))
    df = df.filter((col(colName) < col('lower_bound_'+colName)) | (col(colName) > col('upper_bound_'+colName)))
    df = df.drop('mean_'+colName, 'stddev_'+colName, 'wind-speed', 'wind-direction')
    return df


# Filter data for the last n days
def filter_for_last_n_days(df: dataframe.DataFrame, n: int, last_upload_times: None or datetime) -> dataframe.DataFrame:
    df = df.filter(df.timestamp > last_upload_times - timedelta(days=n))
    return df


# Filter out the data that has already been uploaded
def filter_for_new_data(df: dataframe.DataFrame, last_upload_times: None or datetime) -> dataframe.DataFrame:
    df_filtered = df if last_upload_times == None else df.filter(df.timestamp > last_upload_times)
    return df_filtered


# Upload the data to the database
def upload_data_to_sql(df: dataframe.DataFrame, table_name: str) -> None:
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}/{DB_NAME}") \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .save(mode='append')
    except Exception as e:
        logger.error(f"Error uploading data to {table_name}: {e}")


# Write the data to a csv file overwriting the existing file
def write_to_csv(df: dataframe.DataFrame, file_name: str) -> None:
    try:
        df.write.mode('overwrite').option("header", "true").csv(file_name)
    except Exception as e:
        logger.error(f"Error writing data to csv file: {e}") 


# Clean data
def clean_data(df: dataframe.DataFrame) -> dataframe.DataFrame:
    columns = ['wind_speed', 'wind_direction', 'power_output']
    for colName in columns:
        df = fill_nas_with_mean(df, colName)
        df = replace_outliers_with_mean(df, colName)
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

    # Get the last uploaded time
    last_uploaded_dates = get_last_times()
    logger.info(f'Last upload time: {last_uploaded_dates} gotten')

    # Read the CSV files into a DataFrames
    sdf1 = spark.read.csv(sdf1_path, header=True, inferSchema=True)
    sdf2 = spark.read.csv(sdf2_path, header=True, inferSchema=True)
    sdf3 = spark.read.csv(sdf3_path, header=True, inferSchema=True)
    logger.info('CSV files read into DataFrames')

    # Filter for the last 30 days
    sdf1 = filter_for_last_n_days(sdf1, 30, last_uploaded_dates)
    sdf2 = filter_for_last_n_days(sdf2, 30, last_uploaded_dates)
    sdf3 = filter_for_last_n_days(sdf3, 30, last_uploaded_dates)
    logger.info('Data filtered for the last 30 days')

    # Write the filtered data to csv files
    # write_to_csv(sdf1, sdf1_path)
    # write_to_csv(sdf2, sdf2_path)
    # write_to_csv(sdf3, sdf3_path)
    # logger.info('Filtered data written to csv files')

    # Join the DataFrames
    sdf = sdf1.union(sdf2).union(sdf3)
    logger.info('DataFrames joined')

    # Remove rows where timestamp is null
    sdf = sdf.filter(sdf.timestamp.isNotNull())
    logger.info('Rows with null timestamp removed')

    # Remove duplicate rows
    sdf = remove_duplicate_rows(sdf, ['timestamp', 'turbine_id'])
    logger.info('Duplicate rows removed')

    # Add a column for the date and time
    sdf = sdf.withColumn('date', (col('timestamp')).cast('date'))
    sdf = sdf.withColumn('time', to_timestamp('timestamp', 'HH:mm:ss'))
    logger.info('Date and time columns added')

    # Clean the data
    clean_df = clean_data(sdf)
    logger.info('Data cleaned')

    # Calculate the anomalies for each turbine
    anomalies_df = detect_anomalies(clean_df, 'power_output')
    logger.info('Anomalies detected')

    # Filter out the data that has already been uploaded
    filtered_raw_df = filter_for_new_data(sdf, last_uploaded_dates)
    filtered_clean_df = filter_for_new_data(clean_df, last_uploaded_dates)
    filtered_anomalies_df = filter_for_new_data(anomalies_df, last_uploaded_dates)
    filtered_anomalies_df = filtered_anomalies_df.drop('wind_speed', 'wind_direction')
    logger.info('New data filtered')

    # Upload the raw and processed dataframes to the database
    upload_data_to_sql(filtered_raw_df, 'turbine_data_raw')
    upload_data_to_sql(filtered_clean_df, 'turbine_data_cleaned')
    logger.info('Data uploaded')

    # Calculate the daily statistics for each turbine
    daily_stats_df = calculate_daily_stats(filtered_clean_df, 'power_output')
    logger.info('Daily statistics calculated')

    # Upload the daily statistics and anomalies to the database
    upload_data_to_sql(daily_stats_df, 'summary_statistics')
    logger.info('Daily statistics uploaded')

    # Upload the anomalies to the database
    upload_data_to_sql(filtered_anomalies_df, 'anomalies')
    logger.info('Anomalies uploaded')

    # Stop Spark Session
    spark.stop()
    logger.info('Spark Session stopped')