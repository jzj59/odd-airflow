from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from options.helpers.get_option_chain import fetch_and_land_option_data


default_args = {
    "owner": "JJ",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 1),
    "email": ["jasonzjea@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("options", default_args=default_args, schedule_interval=timedelta(1))

fetch_and_land_option_data = PythonOperator(
    task_id='fetch_and_land_option_data',
    python_callable=fetch_and_land_option_data,
    dag=dag,
    op_kwargs={
        "symbol": "USO",
        "client_id": "SZKIIQY0STUI4WGFAQCVLOBJANB61M0H"
    }
)
