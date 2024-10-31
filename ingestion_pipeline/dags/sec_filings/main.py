from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sec_filings.ping_edgar import ping_edgar
from sec_filings.download_filing_as_html import download_submissions_as_html
from sec_filings.convert_html_to_pdf import convert_submissions_to_pdf
from sec_filings.colpali_embedding import embed_submissions

import sys
sys.path.append("/opt/airflow")
from project_config import colpali_config

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 0
    }

with DAG(
    'edgar_data_ingestion',
    default_args=default_args,
    description='Routine function to ping SEC for new filings',
    schedule_interval='@daily', 
    render_template_as_native_obj=True
    ) as dag:
    
    ping_edgar = PythonOperator(
        task_id = "ping_edgar",
        python_callable = ping_edgar,
        dag = dag
        )

    download_submissions_as_html = PythonOperator(
        task_id='download_submissions_as_html',
        python_callable=download_submissions_as_html,
        # Remove the templated string quotes since we want to pass the actual dictionary
        op_kwargs={
            'submissions_dict': "{{ task_instance.xcom_pull(task_ids='ping_edgar') }}"},
        dag=dag
        )

    convert_submissions_to_pdf = PythonOperator(
        task_id='convert_submissions_to_pdf',
        python_callable=convert_submissions_to_pdf,
        op_kwargs={
            'submissions_dict': "{{ task_instance.xcom_pull(task_ids='download_submissions_as_html') }}"
            },
        dag=dag
        )
    
    embed_submissions = PythonOperator(
        task_id = "embed_submissions",
        python_callable = embed_submissions,
        op_kwargs={
            'submissions_dict': "{{ task_instance.xcom_pull(task_ids='convert_submissions_to_pdf') }}"
            },
        dag = dag
        )
    

    ping_edgar >> download_submissions_as_html >> convert_submissions_to_pdf  >> embed_submissions