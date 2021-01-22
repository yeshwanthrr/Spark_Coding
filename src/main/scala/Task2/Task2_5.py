#Commenting because this is a dummy Dag


'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import pendulum

local_tz = pendulum.timezone("UTC")
default_args = {
    'owner': 'Spark Jobs',
    'depends_on_past': False,
    'start_date': datetime(2021, 01, 22, tzinfo=local_tz),
    'email': ['test@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='Task 2 Spark jobs',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

spark_config = {"conn_id": "spark_local", "java_class": ("com.src.main.spark.Task2_1",),
                "application": "./spark_code.jar", "driver_memory": "1g", "executor_core": 1, "num_executors": 1,
                "executor_memory": "1g", "packages": ""}

#Task2_1
spark_operator_task1 = SparkSubmitOperator(task_id="spark_submit_Task2_1",dag = dag, **spark_config)

#Task2_2
spark_config["java_class"] = "com.src.main.spark.Task2_2",
spark_operator_task2 = SparkSubmitOperator(task_id="spark_submit_Task2_2",dag = dag, **spark_config)

#Task2_3
spark_config["java_class"] = "com.src.main.spark.Task2_3",
spark_operator_task3 = SparkSubmitOperator(task_id="spark_submit_Task2_3",dag = dag, **spark_config)

#Task2_4
spark_config["java_class"] = "com.src.main.spark.Task2_4",
spark_operator_task4 = SparkSubmitOperator(task_id="spark_submit_Task2_4",dag = dag, **spark_config)

#Task2_5
spark_config["java_class"] = "com.src.main.spark.Task2_5",
spark_operator_task5 = SparkSubmitOperator(task_id="spark_submit_Task2_5",dag = dag, **spark_config)

#Task2_6
spark_config["java_class"] = "com.src.main.spark.Task6",
spark_operator_task6 = SparkSubmitOperator(task_id="spark_submit_Task2_6",dag = dag, **spark_config)

#Execution order
spark_operator_task1>>[spark_operator_task2,spark_operator_task3]>>[spark_operator_task4,spark_operator_task5,spark_operator_task6]

'''