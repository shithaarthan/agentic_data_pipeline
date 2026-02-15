"""
Airflow DAG for Lakehouse Pipeline
Orchestrates Bronze → Silver → Gold transformations daily.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lakehouse.pipeline import LakehousePipeline
from data_quality.validation_runner import DataQualityValidator


default_args = {
    'owner': 'foxa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_bronze_layer(**context):
    """Run Bronze layer ingestion."""
    pipeline = LakehousePipeline()
    results = pipeline.run_bronze_layer()
    return results


def run_silver_layer(**context):
    """Run Silver layer transformations."""
    pipeline = LakehousePipeline()
    results = pipeline.run_silver_layer()
    return results


def run_gold_layer(**context):
    """Run Gold layer analytics."""
    pipeline = LakehousePipeline()
    results = pipeline.run_gold_layer()
    return results


def validate_bronze(**context):
    """Validate Bronze layer data quality."""
    validator = DataQualityValidator()
    results = validator.validate_bronze_ohlcv()
    if not results.get('success'):
        raise ValueError(f"Bronze validation failed: {results}")
    return results


def validate_silver(**context):
    """Validate Silver layer data quality."""
    validator = DataQualityValidator()
    results = validator.validate_silver_indicators()
    if not results.get('success'):
        raise ValueError(f"Silver validation failed: {results}")
    return results


# Define DAG
with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Daily Lakehouse ETL Pipeline',
    schedule_interval='0 18 * * *',  # Run at 6 PM daily (after market close)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lakehouse', 'etl', 'trading'],
) as dag:
    
    # Task 1: Bronze Layer
    bronze_task = PythonOperator(
        task_id='bronze_layer',
        python_callable=run_bronze_layer,
    )
    
    # Task 2: Validate Bronze
    validate_bronze_task = PythonOperator(
        task_id='validate_bronze',
        python_callable=validate_bronze,
    )
    
    # Task 3: Silver Layer
    silver_task = PythonOperator(
        task_id='silver_layer',
        python_callable=run_silver_layer,
    )
    
    # Task 4: Validate Silver
    validate_silver_task = PythonOperator(
        task_id='validate_silver',
        python_callable=validate_silver,
    )
    
    # Task 5: Gold Layer
    gold_task = PythonOperator(
        task_id='gold_layer',
        python_callable=run_gold_layer,
    )
    
    # Task 6: dbt models
    dbt_task = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project && dbt build',
    )
    
    # Define dependencies
    bronze_task >> validate_bronze_task >> silver_task >> validate_silver_task >> gold_task >> dbt_task