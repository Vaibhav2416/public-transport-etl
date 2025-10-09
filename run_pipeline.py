"""
Main entry point for the ETL pipeline
"""
from src.etl_pipeline import TransportDataETL

if __name__ == "__main__":
    etl_pipeline = TransportDataETL()
    etl_pipeline.run_etl_pipeline()