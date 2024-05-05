from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark-master:7077"
spark_app_name = "Spark Hello World"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="SparkPi", 
        description="This DAG runs a simple SparkPi.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit --class org.apache.spark.examples.SparkPi --master spark://spark-master:7077 --deploy-mode client --driver-memory 8G --driver-cores 4 --executor-memory 8G --executor-cores 4 /usr/local/spark/app/spark-examples_2.11-2.4.5.jar 10',
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end