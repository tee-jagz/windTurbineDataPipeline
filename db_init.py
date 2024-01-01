import psycopg2
from psycopg2 import sql
import dotenv
import os

dotenv.load_dotenv()


DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

# Database connection parameters
db_params = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST
}

# SQL statements for creating tables
create_tables_commands = (
    """
    CREATE TABLE IF NOT EXISTS turbine_data_raw (
        timestamp TIMESTAMP NOT NULL,
        turbine_id INTEGER NOT NULL,
        wind_speed REAL,
        wind_direction INTEGER,
        power_output REAL,
        PRIMARY KEY (timestamp, turbine_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS turbine_data_cleaned (
        timestamp TIMESTAMP NOT NULL,
        turbine_id INTEGER NOT NULL,
        wind_speed REAL,
        wind_direction INTEGER,
        power_output REAL,
        PRIMARY KEY (timestamp, turbine_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS summary_statistics (
        stat_id SERIAL PRIMARY KEY,
        turbine_id INTEGER NOT NULL,
        min_power_output REAL,
        max_power_output REAL,
        avg_power_output REAL,
        timestamp_from TIMESTAMP NOT NULL,
        timestamp_to TIMESTAMP NOT NULL,
        PRIMARY KEY (stat_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS anomalies (
        anomaly_id SERIAL PRIMARY KEY,
        turbine_id INTEGER NOT NULL,
        power_output REAL,
        lower_bound_power_output REAL,
        upper_bound_power_output REAL,
        timestamp TIMESTAMP NOT NULL,
        calc_days INTEGER NOT NULL,
        PRIMARY KEY (anomaly_id)
   )
    """
)

# Establishing the connection
conn = psycopg2.connect(**db_params)
conn.autocommit = True
cursor = conn.cursor()

# Creating tables
for command in create_tables_commands:
    cursor.execute(command)

# Closing the connection
cursor.close()
conn.close()
