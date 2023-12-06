import pyspark.sql
from pyspark.sql import SparkSession
import argparse
import os
import psycopg2



class Source:
    def __init__(self, file_name, type):
        self.file_name = file_name
        self.type = type
        self.source = os.path.join(os.path.dirname(__file__), f"..\\dataset\\{file_name}.{type}")

def read_from_csv(spark_session, csv_file_path):
    df = spark_session.read.csv(csv_file_path, header=True, inferSchema = True)
    return df

def write_to_db(df, jdbc_url, jdbc_properties, table_name, mode ="append"):
    df.write \
       .jdbc(url = jdbc_url, properties = jdbc_properties, table = table_name, mode = mode)


def run():
    spark = SparkSession.builder \
    .appName("RunTest") \
    .getOrCreate()

    csv_flights = Source("nyc_flights", "csv")
    csv_airlines = Source("nyc_airlines", "csv")
    csv_airports = Source("nyc_airports", "csv")
    csv_planes = Source("nyc_planes", "csv")
    
    df_flights = read_from_csv(spark,  csv_flights.source)
    df_airlines = read_from_csv(spark,  csv_airlines.source)
    df_airports = read_from_csv(spark,  csv_airports.source) \
        .withColumnRenamed("lat","latitude") \
        .withColumnRenamed("lon","longitude") \
        .withColumnRenamed("alt","altitude") \
        .withColumnRenamed("tz","timezone") \
        .withColumnRenamed("tzone","timezone_name")\

    
    df_planes = read_from_csv(spark,  csv_planes.source)

    jdbc_url = "jdbc:postgresql://localhost:5432/dw_flights"
    jdbc_properties = {
        "user" : "postgres",
        "password": "Password1234**",
        "driver": "org.postgresql.Driver"
    } 

    write_to_db(df_airlines, jdbc_url, jdbc_properties, "airlines")
    write_to_db(df_airports, jdbc_url, jdbc_properties, "airports")
    write_to_db(df_planes, jdbc_url, jdbc_properties, "planes")
    write_to_db(df_flights, jdbc_url, jdbc_properties, "flights")

run()