import boto3
import os
import numpy as np
from pyspark.sql.functions import (
    to_date, col, to_timestamp, hour, unix_timestamp, 
    rand, floor, avg, count, sum, round, when
)
from spark_s3_config import create_spark_session, get_s3_path


class TransportDataETL:
    def __init__(self):
        # S3 Configuration
        self.SOURCE_BUCKET = "vaibhav-dev-transport-data-lake"
        self.SOURCE_WEATHER = "staging/weather-data-new-york.csv"
        self.SOURCE_ZONE = "staging/nyc_taxi_zone_lookup.csv"
        self.SOURCE_TRANSPORT = "staging/transport_data_2016-03.csv"
        
        # Local paths
        self.RAW_DIR = "data"
        self.WEATHER_PATH = os.path.join(self.RAW_DIR, "weather-data-new-york.csv")
        self.ZONE_PATH = os.path.join(self.RAW_DIR, "nyc_taxi_zone_lookup.csv")
        self.TRANSPORT_PATH = os.path.join(self.RAW_DIR, "transport_data_2016-03.csv")
        
        # S3 client
        self.s3 = boto3.client("s3")

    def extract_data(self):
        """
        Extract: Download source data from S3 to local storage
        """
        print("🚀 Starting Data Extraction...")
        os.makedirs(self.RAW_DIR, exist_ok=True)
        
        # Download all source files
        files_to_download = [
            (self.SOURCE_WEATHER, self.WEATHER_PATH),
            (self.SOURCE_ZONE, self.ZONE_PATH),
            (self.SOURCE_TRANSPORT, self.TRANSPORT_PATH)
        ]
        
        for source_key, local_path in files_to_download:
            print(f"📥 Downloading s3://{self.SOURCE_BUCKET}/{source_key} to {local_path}")
            self.s3.download_file(self.SOURCE_BUCKET, source_key, local_path)
        
        print("✅ Data Extraction Completed!")
        return True

    def transform_data(self, spark):
        """
        Transform: Process and clean data, apply business logic
        """
        print("🔄 Starting Data Transformation...")
        
        # Read raw data
        df_weather = self._process_weather_data(spark)
        df_transport = self._process_transport_data(spark)
        
        # Join datasets
        df_joined = self._join_datasets(df_transport, df_weather)
        
        # Create business summary
        daily_summary = self._create_daily_summary(df_joined)
        
        print("✅ Data Transformation Completed!")
        return daily_summary

    def _process_weather_data(self, spark):
        """Process and clean weather data"""
        print("🌤️ Processing Weather Data...")
        
        df_weather = spark.read.csv(self.WEATHER_PATH, header=True, inferSchema=True)
        
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
        print("🚕 Processing Transport Data...")
        
        df_transport = spark.read.csv(self.TRANSPORT_PATH, header=True, inferSchema=True)
        
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
            .withColumn("passenger_count", (floor(rand() * 11) + 30).cast("int"))  # Simulate bus data
            .withColumn("bus_number", (floor(rand() * 101) + 100).cast("int"))     # Simulate bus numbers
        )
        
        # Data quality filters
        df_transport = df_transport.filter(
            (col("passenger_count") > 0) &
            (col("trip_distance") > 0) &
            (col("fare_amount") > 0)
        )
        
        print("📊 Transport Data Sample:")
        df_transport.select("pickup_datetime", "dropoff_datetime", "trip_duration_min", "bus_number").show(5)
        
        return df_transport

    def _join_datasets(self, df_transport, df_weather):
        """Join transport and weather data"""
        print("🔗 Joining Transport and Weather Data...")
        
        df_joined = df_transport.join(
            df_weather, 
            df_transport.pickup_date == df_weather.date, 
            "left"
        )
        
        print(f"✅ Joined dataset count: {df_joined.count()}")
        return df_joined

    def _create_daily_summary(self, df_joined):
        """Create daily business summary aggregations"""
        print("📈 Creating Daily Business Summary...")
        
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

    def load_data(self, transformed_data):
        """
        Load: Write transformed data to S3 in optimized format
        """
        print("💾 Starting Data Load...")
        
        s3_path = get_s3_path("vaibhav-dev-transport-data-lake", "processed")
        
        try:
            # Write with partitioning for optimal query performance
            (transformed_data
                .repartition(8)
                .write
                .mode("overwrite")
                .partitionBy("pickup_date")
                .parquet(s3_path)
            )
            
            print(f"🎉 ETL Job Completed Successfully!")
            print(f"📁 Data written to: {s3_path}")
            print(f"📊 Partitioned by: pickup_date")
            
            # Verification
            self._verify_data_load(s3_path)
            
        except Exception as e:
            print(f"❌ Data Load Failed: {str(e)}")
            raise

    def _verify_data_load(self, s3_path):
        """Verify data was written successfully"""
        print("🔍 Verifying Data Load...")
        
        spark = create_spark_session(
            app_name="DataVerification",
            master="local[*]",
            extra_configs={
                "spark.executor.memory": "1g",
                "spark.driver.memory": "1g"
            }
        )
        
        try:
            verify_df = spark.read.parquet(s3_path)
            record_count = verify_df.count()
            partition_count = len(verify_df.select("pickup_date").distinct().collect())
            
            print(f"✅ Verification Successful!")
            print(f"📊 Records loaded: {record_count}")
            print(f"📅 Partitions created: {partition_count}")
            
        finally:
            spark.stop()

    def run_etl_pipeline(self):
        """
        Main ETL pipeline execution
        """
        print("=" * 50)
        print("🚀 STARTING TRANSPORT DATA ETL PIPELINE")
        print("=" * 50)
        
        # Initialize Spark session
        spark = create_spark_session(
            app_name="TransportDataETL",
            master="local[*]",
            extra_configs={
                "spark.executor.memory": "2g",
                "spark.driver.memory": "2g"
            }
        )
        
        try:
            # ETL Process
            self.extract_data()                    # Extract
            transformed_data = self.transform_data(spark)  # Transform  
            self.load_data(transformed_data)       # Load
            
            print("=" * 50)
            print("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 50)
            
        except Exception as e:
            print(f"❌ ETL Pipeline Failed: {str(e)}")
            raise
            
        finally:
            spark.stop()
            print("✅ Spark session stopped")


if __name__ == "__main__":
    # Run the ETL pipeline
    etl_pipeline = TransportDataETL()
    etl_pipeline.run_etl_pipeline()