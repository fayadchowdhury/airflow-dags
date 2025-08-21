from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'fayad',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id = "sample_dag",
    description = "Sample DAG",
    start_date = datetime(2021, 1, 1, 0),
    schedule='@daily',
    default_args=default_args,
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World"',
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "This is the second task"',
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "This is the third task"',
    )

    # Method 1: Use set_downstream() method
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Method 2: Use >> operator
    # task1 >> task2
    # task1 >> task3

    # Method 3: Use set_downstream() method with multiple tasks
    # task1.set_downstream([task2, task3])

    # Method 4: Use >> operator with multiple tasks
    task1 >> [task2, task3]

# The sample_dag.py file contains a DAG with two tasks. The first task, task1, runs a Bash command that prints "Hello World" to the console. The second task, task2, runs a Bash command that prints "This is the second task" to the console. The task1 task is set downstream of the task2 task.
# The task2 and task3 tasks are set downstream of the task1 task.
