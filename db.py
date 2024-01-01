import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

#establishing the connection
conn = psycopg2.connect(
   database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port= '5432'
)
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Preparing query to create a database
sql = '''CREATE database windTurbine_db'''

#Creating a database
cursor.execute(sql)
print("Database created successfully........")

#Closing the connection
conn.close()