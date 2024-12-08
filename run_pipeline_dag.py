from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import subprocess
import logging

# Set up logging for the DAG script
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_earliest_date_from_csv(file_path):
    """Get the earliest date from the CSV file's 'date' column."""
    logging.debug(f"Reading CSV file from path: {file_path}")
    try:
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
        earliest_date = df['date'].min()
        logging.debug(f"Earliest date found in {file_path}: {earliest_date}")
        return earliest_date
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise

def run_pipeline(execution_date, **context):
    """Run the pipeline script using subprocess."""
    try:
        # Ensure execution_date is a datetime object (it should already be, but let's make sure)
        if isinstance(execution_date, str):
            logging.debug(f"Execution date is a string: {execution_date}. Converting to datetime.")
            execution_date = pd.to_datetime(execution_date)  # Convert if it's passed as a string

        formatted_execution_date = execution_date.strftime('%Y-%m-%d')  # Convert to string for passing as argument to the script
        logging.debug(f"Formatted execution_date for subprocess: {formatted_execution_date}")

        # Now pass the execution_date correctly to the script
        logging.debug(f"Running subprocess with execution_date: {formatted_execution_date}")
        result = subprocess.run(
            ['/home/ubuntu/tp1_env/bin/python3', '/home/ubuntu/airflow/dags/Pipeline_test.py', formatted_execution_date],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.debug(f"Subprocess executed successfully. STDOUT: {result.stdout}")
        logging.debug(f"Subprocess STDERR: {result.stderr}")
    
    except subprocess.CalledProcessError as e:
        logging.error(f"Subprocess error occurred: {e}")
        logging.error(f"Subprocess STDOUT: {e.stdout}")
        logging.error(f"Subprocess STDERR: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"Error in run_pipeline: {e}")
        raise

def get_dynamic_start_date():
    """Get the earliest date from the CSVs."""
    product_log_file = "/home/ubuntu/product_views.csv"
    ads_log_file = "/home/ubuntu/ads_views.csv"

    # Find the earliest date in both CSVs
    logging.debug("Fetching dynamic start date from CSV files.")
    try:
        earliest_product_date = get_earliest_date_from_csv(product_log_file)
        earliest_ads_date = get_earliest_date_from_csv(ads_log_file)
        earliest_date = min(earliest_product_date, earliest_ads_date)
        logging.debug(f"Earliest date from both CSVs: {earliest_date}")
        return earliest_date
    except Exception as e:
        logging.error(f"Error in get_dynamic_start_date: {e}")
        raise

# Dynamically fetch the start date
dynamic_start_date = get_dynamic_start_date()

# Define the DAG
with DAG(
    dag_id='run_pipeline_dag',
    start_date=dynamic_start_date,
    schedule_interval='@daily',  # Daily scheduling
    catchup=True,  # Enable backfilling
) as dag:

    # Define the PythonOperator task
    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
        op_kwargs={'execution_date': '{{ ds }}'},  # Pass execution_date using Airflow macros
        provide_context=True  # Pass context to allow automatic `execution_date` handling
    )

