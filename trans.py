from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace, lit, substring, expr, current_timestamp, date_format, coalesce , sum, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType , LongType , IntegerType
import sys

sys.stdout = open('out.log','w') 

# Function to establish a connection to MySQL
def connect_to_mysql():
    spark = SparkSession.builder \
        .appName("MySQL-Spark connection") \
        .config("spark.jars", "./mysql-connector-j-9.0.0.jar") \
        .getOrCreate()
    
    # Define MySQL connection URL
    mysql_url = "jdbc:mysql://localhost:3306/project_fp"
    
    # Define MySQL connection properties (user, password, driver)
    mysql_properties = {
        "user": "root",
        "password": "W7301@jpir#",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    return spark, mysql_url, mysql_properties

def read_data(spark, csv_file_path):
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    return df

# First transformation function
def transform1(df):
    # Rename columns to make them more meaningful
    df = df.withColumnRenamed("Time", "rdr_Time") \
           .withColumnRenamed("host", "rdr_Host") \
           .withColumnRenamed("Roaming_Flag", "rdr_Roaming_Flag") \
           .withColumnRenamed("Total_Drops", "rdr_Data_Volume")
    
    # Handle null values by putting default values
    df = df.withColumn("rdr_Time", when((col("rdr_Time").isNull()) | (col("rdr_Time") == ""), "19700101000000").otherwise(col("rdr_Time")))
    df = df.withColumn("rdr_Host", when((col("rdr_Host").isNull()) | (col("rdr_Host") == ""), "_N").otherwise(col("rdr_Host")))
    df = df.withColumn("rdr_Roaming_Flag", when((col("rdr_Roaming_Flag").isNull()) | (col("rdr_Roaming_Flag") == ""), "_N").otherwise(col("rdr_Roaming_Flag")))
    df = df.withColumn("rdr_Data_Volume", when((col("rdr_Data_Volume").isNull()) | (col("rdr_Data_Volume") == ""), "0").otherwise(col("rdr_Data_Volume")))
    
    # Remove spaces 
    df = df.withColumn("rdr_Time", regexp_replace(trim(col("rdr_Time")), "[- :]", ""))
    df = df.withColumn("rdr_Host", trim(col("rdr_Host")))
    df = df.withColumn("rdr_Roaming_Flag", trim(col("rdr_Roaming_Flag")))
    df = df.withColumn("rdr_Data_Volume", trim(col("rdr_Data_Volume")))
    
    constant_fields = {
        "DIM_ENTITY": "GGSN_COUNTERS",
        "DIM_MONITORING_POINT": "_N",
        "DIM_FILENAME": "_N",
        "DIM_START_DATE": "19700101",
        "DIM_START_HOUR": "00",
        "DIM_START_MIN": "00",
        "DIM_PROC_DATE": "19700101",
        "DIM_PROC_HOUR": "00",
        "DIM_LOCATION_IDENTIFIER": "_N",
        "DIM_APN": "_N",
        "DIM_NODE_NAME": "_N",
        "DIM_FLAG_2G_3G": "_N",
        "MES_DATA_VOLUME": 0,
        "MES_DOWNLINK_VOLUME": 0,
        "MES_UPLINK_VOLUME": 0,
        "MES_CDR_COUNT": 0
    }
    
    # Iterate over constant fields and add them to DataFrame
    for field, value in constant_fields.items():
        df = df.withColumn(field, lit(value))
    
    return df

# Second transformation function
def transform2(df):    
    # Add current timestamp for processing date and time
    current_date = date_format(current_timestamp(), "yyyyMMddHHmmss")
    df = df.withColumn("DIM_PROC_DATE", substring(current_date, 1, 8))
    df = df.withColumn("DIM_PROC_HOUR", substring(current_date, 9, 2))
    
    # Update start time, date, hour, and minute fields
    df = df.withColumn("DIM_START_TIME", col("rdr_Time"))
    df = df.withColumn("DIM_START_DATE", substring(col("DIM_START_TIME"), 1, 8))
    df = df.withColumn("DIM_START_HOUR", substring(col("DIM_START_TIME"), 9, 2))
    df = df.withColumn("DIM_START_MIN", substring(col("DIM_START_TIME"), 11, 2))
    
    # Logic for DIM_LOCATION_IDENTIFIER based on roaming flag
    df = df.withColumn("DIM_LOCATION_IDENTIFIER", 
                       when(col("rdr_Roaming_Flag") == "0", "1")
                       .when(col("rdr_Roaming_Flag") == "1", "3")
                       .otherwise(col("DIM_LOCATION_IDENTIFIER")))
    
    # Convert data volume to double
    df = df.withColumn("MES_DATA_VOLUME", col("rdr_Data_Volume").cast(DoubleType()))
    
    # Rename columns back to the original names for final schema
    df = df.withColumnRenamed("rdr_Time", "Time") \
           .withColumnRenamed("rdr_Host", "host") \
           .withColumnRenamed("rdr_Roaming_Flag", "Roaming_Flag") \
           .withColumnRenamed("rdr_Data_Volume", "Total_Drops")    
           
    return df

def write_to_mysql(df, table_name, mysql_url, mysql_properties):
    try:
        df.write.jdbc(url=mysql_url, table=table_name, mode="append", properties=mysql_properties)
        print(f"Data successfully written to table: {table_name}")
    except Exception as e:
        print(f"An error occurred while writing to MySQL: {e}")
        df.printSchema()  

def transform3(df):
    for column in df.columns:
        df = df.withColumn(column, trim(col(column)))
    
    nvl_rules = {
        "DIM_START_DATE": ("19700101", StringType()),
        "DIM_START_HOUR": ("00", StringType()),
        "DIM_PROC_DATE": ("19700101", StringType()),
        "DIM_PROC_HOUR": ("00", StringType()),
        "DIM_LOCATION_IDENTIFIER": ("_N", StringType()),
        "MES_DATA_VOLUME": ("0", StringType()),
        "DIM_START_MIN": ("00", StringType())
    }
    
    for column, (default, data_type) in nvl_rules.items():
        df = df.withColumn(column, coalesce(col(column), lit(default)).cast(data_type))
    
    df = df.select(
        "DIM_MONITOrING_POINT",
        "DIM_START_DATE",
        "DIM_START_HOUR",
        "DIM_PROC_DATE",
        "DIM_PROC_HOUR",
        "DIM_FILENAME",
        "DIM_LOCATION_IDENTIFIER",
        "MES_CDR_COUNT",
        "MES_DATA_VOLUME",
        "MES_DOWNLINK_VOLUME",
        "MES_UPLINK_VOLUME",
        "DIM_APN",
        "DIM_NODE_NAME",
        "DIM_FLAG_2G_3G",
        "DIM_START_MIN"
    )
        
    return df

def aggregate_data(df):
    # Define the dimensions 
    dimensions = [
        "DIM_APN",
        "DIM_FLAG_2G_3G",
        "DIM_LOCATION_IDENTIFIER",
        "DIM_MONITOrING_POINT",
        "DIM_NODE_NAME",
        to_date(col("DIM_START_DATE"), "yyyyMMdd").alias("START_DATE"),
        "DIM_START_HOUR",
        "DIM_START_MIN"
    ]

    measures = [
        sum(when(col("MES_CDR_COUNT") == "_N", 0).otherwise(col("MES_CDR_COUNT").cast(IntegerType()))).alias("CDR_COUNT"),
        sum(when(col("MES_DATA_VOLUME") == "_N", 0).otherwise(col("MES_DATA_VOLUME").cast(DoubleType()))).alias("DATA_VOLUME"),
        sum(when(col("MES_DOWNLINK_VOLUME") == "_N", 0).otherwise(col("MES_DOWNLINK_VOLUME").cast(DoubleType()))).alias("DOWNLINK_VOLUME"),
        sum(when(col("MES_UPLINK_VOLUME") == "_N", 0).otherwise(col("MES_UPLINK_VOLUME").cast(DoubleType()))).alias("UPLINK_VOLUME")
    ]

    df_aggregated = df.groupBy(*dimensions).agg(*measures)

    # Rename the columns to match the destination table
    df_aggregated = df_aggregated.select(
        col("DIM_APN").alias("APN"),
        col("DIM_FLAG_2G_3G").alias("FLAG_2G_3G"),
        col("DIM_LOCATION_IDENTIFIER").alias("LOCATION_IDENTIFIER"),
        col("DIM_MONITOrING_POINT").alias("MONITORING_POINT"),
        col("DIM_NODE_NAME").alias("NODE_NAME"),
        col("START_DATE"),
        col("DIM_START_HOUR").alias("START_HOUR"),
        col("DIM_START_MIN").alias("START_MIN"),
        "CDR_COUNT",
        "DATA_VOLUME",
        "DOWNLINK_VOLUME",
        "UPLINK_VOLUME"
    )
    
    return df


def main():
    try:
        spark, mysql_url, mysql_properties = connect_to_mysql()
        csv_file_path = "./revenue_assurance_Drops_2024-05-28.csv"
        
        # Read the data
        df = read_data(spark, csv_file_path)
        print("Original DataFrame:")
        df.show(5)
        
        # Apply transformations
        df_transformed1 = transform1(df)
        print("After transform1:")
        df_transformed1.show(5)
        
        df_transformed2 = transform2(df_transformed1)
        print("After transform2:")
        df_transformed2.show(5)
        
        df_transformed3 = transform3(df_transformed2)
        print("Final transformed DataFrame:")
        df_transformed3.show(5)
        
        print("Final transformed DataFrame schema:")
        df_transformed3.printSchema()
        
        df_aggregated = aggregate_data(df_transformed3)
        print("Aggregated DataFrame:")
        df_aggregated.show(5)
        
        print("Aggregated DataFrame schema:")
        df_aggregated.printSchema()
        
        # Write to MySQL
        write_to_mysql(df_aggregated, "revenue", mysql_url, mysql_properties)
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            
if __name__ == "__main__":
    main()