## import required libraries
import pyspark
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_URL = os.getenv('DB_URL')

## create spark session (if one already exists, it will be retrieved)
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', r"C:\Users\mginh\Documents\GitHub\data-pipeline-project\jars\postgresql-42.7.7.jar") \
   .getOrCreate()


## read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "DB_URL") \
   .option("dbtable", "movies") \
   .option("user", "DB_USER") \
   .option("password", "DB_PASSWORD") \
   .option("driver", "org.postgresql.Driver") \
   .load()

## print the movies_df
print(movies_df.show())