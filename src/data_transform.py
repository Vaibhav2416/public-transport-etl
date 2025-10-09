"""
Data Transformation Module
Handles data cleaning, processing, and feature engineering
"""
import numpy as np
from pyspark.sql.functions import (
    to_date, col, to_timestamp, hour, unix_timestamp, 
    rand, floor, avg, count, sum, round, when
)
from src.config.aws_config import AWSConfig


class DataTransformer:
    def __init__(self):
        self.config = AWSConfig()
        
    def transform_data(self, spark):
        """
        Transform: Process and clean data, apply business logic
        """
        print("Starting Data Transformation...")
        
        # Read and process data
        df_weather = self._process_weather_data(spark)
        df_transport = self._process_transport_data(spark)
        
        # Join datasets
        df_joined = self._join_datasets(df_transport, df_weather)
        
        # Create business summary
        daily_summary = self._create_daily_summary(df_joined)
        
        print("Data Transformation Completed!")
        return daily_summary

    def _process_weather_data(self, spark):
        """Process and clean weather data"""
        print("Processing Weather Data...")
        
        df_weather = spark.read.csv(self.config.WEATHER_PATH, header=True, inferSchema=True)
        
        # Standardize date format
        df_weather = df_weather.withColumn(
            "date",
            when(
                col("date").rlike(r"^(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\d{4}$"),
                to_date(col("date"), "M-d-yyyy")
            ).otherwise(None)
        )
        
        # Convert numeric columns, handle invalid values
        numeric_cols = ["precipitation", "snow fall", "snow depth"]
        for col_name in numeric_cols:
            df_weather = df_weather.withColumn(
                col_name,
                when(col(col_name).rlike(r"^\d+(\.\d+)?$"), col(col_name).cast("double")).otherwise(np.nan)
            )
        
        print("📊 Weather Data Schema:")
        df_weather.printSchema()
        df_weather.show(5)
        
        return df_weather

    def _process_transport_data(self, spark):
        """Process and clean transport data"""
        print("Processing Transport Data...")
        
        df_transport = spark.read.csv(self.config.TRANSPORT_PATH, header=True, inferSchema=True)
        
        # Parse timestamps
        df_transport = (df_transport
            .withColumn(
                "pickup_datetime",
                when(
                    col("tpep_pickup_datetime").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
                    to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
                ).otherwise(None)
            )
            .withColumn(
                "dropoff_datetime",
                when(
                    col("tpep_dropoff_datetime").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
                    to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
                ).otherwise(None)
            )
        )
        
        # Feature engineering
        df_transport = (df_transport
            .withColumn("pickup_date", to_date(col("pickup_datetime")))
            .withColumn("pickup_hour", hour("pickup_datetime"))
            .withColumn("trip_duration_min", 
                       (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)
            .withColumn("passenger_count", (floor(rand() * 11) + 30).cast("int"))
            .withColumn("bus_number", (floor(rand() * 101) + 100).cast("int"))
        )
        
        # Data quality filters
        df_transport = df_transport.filter(
            (col("passenger_count") > 0) &
            (col("trip_distance") > 0) &
            (col("fare_amount") > 0)
        )
        
        print("Transport Data Sample:")
        df_transport.select("pickup_datetime", "dropoff_datetime", "trip_duration_min", "bus_number").show(5)
        
        return df_transport

    def _join_datasets(self, df_transport, df_weather):
        """Join transport and weather data"""
        print("Joining Transport and Weather Data...")
        
        df_joined = df_transport.join(
            df_weather, 
            df_transport.pickup_date == df_weather.date, 
            "left"
        )
        
        print(f"Joined dataset count: {df_joined.count()}")
        return df_joined

    def _create_daily_summary(self, df_joined):
        """Create daily business summary aggregations"""
        print("Creating Daily Business Summary...")
        
        daily_summary = df_joined.groupBy("pickup_date", "bus_number").agg(
            count("*").alias("total_trips"),
            avg("fare_amount").alias("avg_fare"),
            avg("tip_amount").alias("avg_tip"),
            sum("total_amount").alias("total_revenue"),
            round(avg("precipitation"), 4).alias("avg_precipitation"),
            round(avg("trip_duration_min"), 4).alias("avg_trip_duration")
        )
        
        print("📊 Daily Summary Sample:")
        daily_summary.show(10, False)
        daily_summary.printSchema()
        
        return daily_summary