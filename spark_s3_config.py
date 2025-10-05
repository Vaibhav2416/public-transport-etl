"""
S3 Configuration Module for PySpark
Handles all S3A configuration fixes for PySpark on Windows
"""

import os
import sys
from pyspark.sql import SparkSession
from config import AWSConfig  # Import from config module

def get_s3_configs():
    """
    Returns all S3 configurations with fixed numeric values
    """
    return {
        # Time/duration configurations in milliseconds
        "spark.hadoop.fs.s3a.threads.keepalivetime": "60000",
        "spark.hadoop.fs.s3a.connection.ttl": "300000", 
        "spark.hadoop.fs.s3a.multipart.purge.age": "86400000",
        
        # Retry intervals
        "spark.hadoop.fs.s3a.retry.interval": "500",
        "spark.hadoop.fs.s3a.retry.throttle.interval": "100",
        
        # Connection timeouts
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.request.timeout": "60000",
        
        # Essential S3 settings
        "spark.hadoop.fs.s3a.attempts.maximum": "10",
        "spark.hadoop.fs.s3a.endpoint": f"s3.{AWSConfig.REGION}.amazonaws.com",
        "spark.hadoop.fs.s3a.path.style.access": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        
        # Performance optimizations
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.multipart.size": "67108864",
        "spark.hadoop.fs.s3a.multipart.threshold": "134217728",
        
        # Buffer sizes
        "spark.hadoop.fs.s3a.socket.send.buffer": "8192",
        "spark.hadoop.fs.s3a.socket.recv.buffer": "8192",
        "spark.hadoop.fs.s3a.readahead.range": "65536",
        
        # File system implementation
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }

def configure_python_environment():
    """
    Configure Python environment for PySpark
    """
    python_executable = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable
    return python_executable

def create_spark_session(app_name="S3App", master="local[*]", extra_configs=None):
    """
    Create a Spark session with pre-configured S3 settings
    """
    # Configure Python environment
    configure_python_environment()
    
    # Validate AWS credentials
    try:
        AWSConfig.validate_credentials()
        print("✅ AWS credentials validated successfully")
    except ValueError as e:
        print(f"❌ AWS Configuration Error: {e}")
        raise
    
    # Start Spark builder
    spark_builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Add S3 configurations
    s3_configs = get_s3_configs()
    for key, value in s3_configs.items():
        spark_builder = spark_builder.config(key, value)
    
    # Add AWS credentials securely
    spark_builder = spark_builder \
        .config("spark.hadoop.fs.s3a.access.key", AWSConfig.ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWSConfig.SECRET_KEY)
    
    print("✅ AWS credentials configured securely")
    
    # Add any extra configurations
    if extra_configs:
        for key, value in extra_configs.items():
            spark_builder = spark_builder.config(key, value)
    
    # Create and return Spark session
    spark = spark_builder.getOrCreate()
    
    # Verify critical configurations
    verify_s3_configuration(spark)
    
    return spark

def verify_s3_configuration(spark):
    """
    Verify that S3 configurations are properly set
    """
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    critical_props = [
        "fs.s3a.access.key",
        "fs.s3a.secret.key",
        "fs.s3a.threads.keepalivetime",
        "fs.s3a.connection.ttl", 
    ]
    
    print("🔍 Verifying S3 Configuration:")
    for prop in critical_props:
        value = hadoop_conf.get(prop)
        if "secret" in prop and value:
            value = "***" + value[-4:]  # Mask secret key
        status = "✅" if value else "❌"
        print(f"  {status} {prop} = {value}")
    
    print("✅ S3 configuration verified")

def get_s3_path(bucket_name, prefix=""):
    """
    Helper function to create S3 paths
    """
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    return f"s3a://{bucket_name}/{prefix}"