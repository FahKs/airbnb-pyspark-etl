# import Python libraries to interact with the operating system, manage system-specific parameters
# and handle date&time calculations for the Data cleaning process.
import os
import sys
import datetime

# Resetting Spark-related environment variables to avoid configuration conflicts.
if "SPARK_HOME" in os.environ: del os.environ["SPARK_HOME"]
if "PYSPARK_PYTHON" in os.environ: del os.environ["PYSPARK_PYTHON"]

# importing SparkSession as the entry point for Spark functionality 
# and SQL functions (aliased as 'F') for data transformation and manipulation.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Defining a custom logging function to track application progress with real-time timestamps, facilitating better monitoring 
# and debugging during the data pipeline execution.
def log_status(message):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] STATUS >> {message}")

def clean_airbnb_data(df):
    """Clean function"""
    # Handle missing values by imputing defaults: 'Unknown' for names and 0 for price.
    # Assigning 0 to missing prices to preserve the records for overall analysis, 
    # while marking them for exclusion in subsequent price-related calculations.
    df_cleaned = df.fillna({'name': 'Unknown', 'price': 0})
    return df_cleaned

# Initializing the Spark Session with a custom configuration, including a JDBC driver for PostgreSQL connectivity 
# and local host settings to ensure a stable environment.
def start_spark():
    try:
        jar_path = os.path.abspath("./jars/postgresql-42.7.8.jar")
        
        spark = SparkSession.builder \
            .master("local") \
            .appName("Airbnb_Pro_Pipeline") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.jars", jar_path) \
            .getOrCreate()
        return spark
    except Exception as e:
        log_status(f"ERROR: Failed to initialize Spark Session. Details: {e}")
        return None
    
# Automates the ETL pipeline by ingesting CSV datasets, applying data cleaning logic to specific tables, 
# and loading the processed data into a PostgreSQL database using JDBC.
spark = start_spark()

if spark:
    log_status("Spark Session initialized successfully.")
    files = ['listings', 'neighbourhoods', 'reviews']
    db_url = "jdbc:postgresql://localhost:5432/airbnb_raw"
    db_properties = {
        "user": "admin",
        "password": "password123", 
        "driver": "org.postgresql.Driver"
    }

    for file_name in files:
        path = f"./dataset/raw/{file_name}.csv"
        log_status(f"Ingesting source file: {file_name}")
        
        try:
            df = spark.read.csv(path, header=True, inferSchema=True)
            if file_name == 'listings':
                log_status("INFO: Processing Listings data...")
                df = clean_airbnb_data(df)
            log_status(f"INFO: Exporting {file_name} to PostgreSQL...")
            df.write.jdbc(url=db_url, table=file_name, mode="overwrite", properties=db_properties)
            log_status(f"SUCCESS: {file_name} has been successfully processed.")
            
        except Exception as e:
            log_status(f"FATAL: Error handling {file_name} -> {e}")

else:
    log_status("ERROR: Spark initialization error. Check JAVA_HOME path.")