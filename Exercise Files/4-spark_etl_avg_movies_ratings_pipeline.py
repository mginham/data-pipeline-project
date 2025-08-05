# Summary of this script:
# This script implements a simple ETL (Extract, Transform, Load) pipeline using PySpark and PostgreSQL:
# - Extract: Connects to a PostgreSQL database and extracts two tables (movies and users) as Spark DataFrames.
# - Transform: Computes the average rating for each movie by grouping the users table by movie_id and calculating the mean rating. Then it joins this average rating back to the movies table.
# - Load: Writes the transformed DataFrame (avg_ratings) back into the PostgreSQL database, overwriting the existing table.
# The script uses environment variables for secure database credentials and uses JDBC with the PostgreSQL driver.

# Import required libraries for Spark and environment variable management
import pyspark
from dotenv import load_dotenv
import os

# Load environment variables from .env file to keep credentials secure and flexible
load_dotenv()

# Retrieve database connection details from environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_URL = os.getenv('DB_URL')

# Create or retrieve a SparkSession with PostgreSQL JDBC driver included
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', r"C:\Users\mginh\Documents\GitHub\data-pipeline-project\jars\postgresql-42.7.7.jar") \
   .getOrCreate()

# Function to extract the 'movies' table from the PostgreSQL database into a Spark DataFrame
def extract_movies_to_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "movies") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return movies_df

# Function to extract the 'users' table from the PostgreSQL database into a Spark DataFrame
def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "users") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return users_df

# Function to transform the data by calculating average ratings per movie and joining to movie info
def transform_avg_ratings(movies_df, users_df):
    # Group 'users' data by 'movie_id' and calculate mean of 'rating' column
    avg_rating = users_df.groupBy("movie_id").mean("rating")

    # Join average ratings back to the movies DataFrame on movie ID
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)

    # Drop redundant 'movie_id' column from joined DataFrame
    df = df.drop("movie_id")

    return df

# Function to load the transformed DataFrame into the PostgreSQL database
def load_df_to_db(df):
    mode = "overwrite" # Overwrite existing data in the target table
    url = DB_URL # JDBC URL for the PostgreSQL connection
    properties = {"user": DB_USER,
                  "password": DB_PASSWORD,
                  "driver": "org.postgresql.Driver"
                  }
    # Write DataFrame to PostgreSQL using JDBC with specified mode and connection properties
    df.write.jdbc(url=url,
                  table = "avg_ratings", # Target table name
                  mode = mode,
                  properties = properties)

# Entry point: run the ETL pipeline when script is executed
if __name__ == "__main__":
    # Extract movie and user data from DB
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()

    # Transform the data to calculate average ratings
    ratings_df = transform_avg_ratings(movies_df, users_df)
    
    # Load transformed data back to DB
    load_df_to_db(ratings_df)