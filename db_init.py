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
        Date DATE NOT NULL,
        Time TIME NOT NULL,
        TurbineID INTEGER NOT NULL,
        WindSpeed REAL,
        WindDirection INTEGER,
        PowerOutput REAL,
        PRIMARY KEY (TurbineID, Date, Time)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS turbine_data_cleaned (
        Date DATE NOT NULL,
        Time TIME NOT NULL,
        TurbineID INTEGER NOT NULL,
        WindSpeed REAL,
        WindDirection INTEGER,
        PowerOutput REAL,
        PowerOutputFilled REAL,
        PRIMARY KEY (TurbineID, Date, Time)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS summary_statistics (
        TurbineID INTEGER NOT NULL,
        Date DATE NOT NULL,
        MinPowerOutput REAL,
        MaxPowerOutput REAL,
        AvgPowerOutput REAL,
        PRIMARY KEY (TurbineID, Date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS anomalies (
        TurbineID INTEGER NOT NULL,
        Date DATE NOT NULL,
        Time TIME NOT NULL,
        MeasuredPowerOutput REAL,
        ExpectedRangeLow REAL,
        ExpectedRangeHigh REAL,
        PRIMARY KEY (TurbineID, Date, Time)
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
