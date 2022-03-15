from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from datetime import timedelta, datetime

# These args will get passed on to the python operator
default_args = {
    'owner': 'athreya',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 10),
    'email': ['athreya.praturi@futurense.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the python function:
def my_function1(x):
    return x + " is a must have tool for Data Engineers."

def my_function2(x):
    return x + " is learning Airflow"



# define the DAG
dag = DAG(
    'AP_email_operator',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)
start=DummyOperator(
    task_id='start',
    dag=dag
)
# define the first task
email1 = EmailOperator(
    task_id ='email1',
    to=['athreyapraturi97@gmail.com','akashsjce8050@gmail.com'],
    subject="Airflow Testing",
    html_content=""" You have been awarded 10 crores please provide your details for transfering the money into your account """,
    dag=dag
)


end=DummyOperator(
    task_id='end',
    dag=dag
)


start>>email1>>end
