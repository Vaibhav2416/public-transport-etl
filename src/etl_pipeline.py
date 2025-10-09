"""
Main ETL Pipeline Orchestrator
Coordinates the entire ETL process
"""
from src.data_extract import DataExtractor
from src.data_transform import DataTransformer
from src.data_load import DataLoader
from src.config.spark_config import create_spark_session


class TransportDataETL:
    def __init__(self):
        # self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        
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
            # self.extractor.extract_data()                    # Extract
            transformed_data = self.transformer.transform_data(spark)  # Transform  
            self.loader.load_data(transformed_data)          # Load
            
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