"""
Data Loading Module
Handles writing processed data to destination
"""
from src.config.spark_config import create_spark_session, get_s3_path
from src.config.aws_config import AWSConfig


class DataLoader:
    def __init__(self):
        self.config = AWSConfig()
        
    def load_data(self, transformed_data):
        """
        Load: Write transformed data to S3 in optimized format
        """
        print("Starting Data Load...")
        
        s3_path = get_s3_path(self.config.SOURCE_BUCKET, "processed")
        
        try:
            # Write with partitioning for optimal query performance
            (transformed_data
                .repartition(8)
                .write
                .mode("overwrite")
                .partitionBy("pickup_date")
                .parquet(s3_path)
            )
            
            print(f"ETL Job Completed Successfully!")
            print(f"Data written to: {s3_path}")
            print(f"Partitioned by: pickup_date")
            
            # Verification
            self._verify_data_load(s3_path)
            
        except Exception as e:
            print(f"Data Load Failed: {str(e)}")
            raise

    def _verify_data_load(self, s3_path):
        """Verify data was written successfully"""
        print("Verifying Data Load...")
        
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
            
            print(f"Verification Successful!")
            print(f"Records loaded: {record_count}")
            print(f"Partitions created: {partition_count}")
            
        finally:
            spark.stop()