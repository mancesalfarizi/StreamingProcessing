from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import datetime as dt
import pendulum


d = dt.datetime.now()
local = pendulum.timezone("Asia/Jakarta")
default_args = {
	'owner' : 'Salman',
	'start_date' : dt.datetime(2020, 10, 2, tzinfo = local),
	'retries' :1,
	'concurrency': 5 
}

dag_conf = DAG('bigproject', default_args=default_args, schedule_interval='0 */24 * * *', catchup=False)

def start() :
	print("ayo!!")

def finish():
	print("Job Anda sudah Selesai.")

mulai = PythonOperator(task_id='starting_job', python_callable=start, dag=dag_conf)
trigger = BashOperator(task_id='cleansing_job', \
	bash_command='spark-submit --master yarn --class selalu.Main /home/mancesalfarizi/terus-diputar.jar', dag=dag_conf)
selesai = PythonOperator(task_id='job_finished', python_callable=finish, dag=dag_conf)


 mulai>>trigger>>selesai	