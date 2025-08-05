# Summary of this script:
# This script performs the following steps:
# 1. Loads database connection credentials securely from environment variables.
# 2. Creates a Spark session configured to connect to a PostgreSQL database via JDBC.
# 3. Reads two tables (movies and users) from the PostgreSQL database into Spark DataFrames.
# 4. Prints the original DataFrames for verification.

# Import PySpark and environment variable libraries
import pyspark
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve database credentials and URL from environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_URL = os.getenv('DB_URL')

# Create a Spark session or get the existing one
# - Spark needs a driver JAR file for PostgreSQL to connect via JDBC
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', r"C:\Users\mginh\Documents\GitHub\data-pipeline-project\jars\postgresql-42.7.7.jar") \
   .getOrCreate()

# Read the movies table from db using Spark JDBC
# - Use Spark's JDBC data source to read the movies table from the PostgreSQL database into a Spark DataFrame
movies_df = spark.read \
   .format("jdbc") \
   .option("url", DB_URL) \
   .option("dbtable", "movies") \
   .option("user", DB_USER) \
   .option("password", DB_PASSWORD) \
   .option("driver", "org.postgresql.Driver") \
   .load()

# Read the users table from db using Spark JDBC
# - Use Spark's JDBC data source to read the users table from the PostgreSQL database into a Spark DataFrame
users_df = spark.read \
   .format("jdbc") \
   .option("url", DB_URL) \
   .option("dbtable", "users") \
   .option("user", DB_USER) \
   .option("password", DB_PASSWORD) \
   .option("driver", "org.postgresql.Driver") \
   .load()

# Print the contents of the movies and user DataFrames in the console
print(movies_df.show())
print(users_df.show())