"""
Data Extraction Module
Handles downloading data from S3 to local storage
"""
import boto3
import os
from src.config.aws_config import AWSConfig


class DataExtractor:
    def __init__(self):
        self.s3 = boto3.client("s3")
        self.config = AWSConfig()
        
    def extract_data(self):
        """
        Extract: Download source data from S3 to local storage
        """
        print("Starting Data Extraction...")
        os.makedirs(self.config.RAW_DIR, exist_ok=True)
        
        # Download all source files
        files_to_download = [
            (self.config.SOURCE_WEATHER, self.config.WEATHER_PATH),
            (self.config.SOURCE_ZONE, self.config.ZONE_PATH),
            (self.config.SOURCE_TRANSPORT, self.config.TRANSPORT_PATH)
        ]
        
        for source_key, local_path in files_to_download:
            print(f"📥 Downloading s3://{self.config.SOURCE_BUCKET}/{source_key} to {local_path}")
            self.s3.download_file(self.config.SOURCE_BUCKET, source_key, local_path)
        
        print("✅ Data Extraction Completed!")
        return True