"""
Secure Configuration Management
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class AWSConfig:
    """AWS Configuration with secure credential management"""
    
    # AWS Credentials from environment variables
    ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
    REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # S3 Buckets
    SOURCE_BUCKET = "vaibhav-dev-transport-data-lake"
    PROCESSED_BUCKET = "vaibhav-dev-transport-data-lake"
    
    @classmethod
    def validate_credentials(cls):
        """Validate that AWS credentials are available"""
        if not cls.ACCESS_KEY or not cls.SECRET_KEY:
            raise ValueError(
                "AWS credentials not found. "
                "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables "
                "or create a .env file in your project root."
            )
        return True
    
    @classmethod
    def get_credentials_status(cls):
        """Check if credentials are properly configured"""
        return {
            'access_key_configured': bool(cls.ACCESS_KEY),
            'secret_key_configured': bool(cls.SECRET_KEY),
            'region': cls.REGION
        }