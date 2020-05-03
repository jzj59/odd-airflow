from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from options.helpers.get_option_chain import fetch_and_land_option_data
from airflow.operators.email_operator import EmailOperator


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

email = EmailOperator(
        task_id='send_email',
        to='jasonzjea@gmail.com',
        subject='Successful Run for Options DAG.',
        html_content=""" <h3>Yay!</h3> """,
        dag=dag
)

ticker_list = ['USO', 'APRN']
for symbol in ticker_list:
    option_tasks = PythonOperator(
        task_id=f"{symbol}_options",
        python_callable=fetch_and_land_option_data,
        dag=dag,
        op_kwargs={
            "symbol": symbol,
            "client_id": "SZKIIQY0STUI4WGFAQCVLOBJANB61M0H"
        }
    )

    option_tasks >> email

