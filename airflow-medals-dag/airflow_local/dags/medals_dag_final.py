from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.providers.common.sql.sensors.sql import SqlSensor
import random
import time

def generate_random_medal():
    medal_types = ['Gold', 'Silver', 'Bronze']
    return random.choice(medal_types)
    
def calk_medal(**kwargs):
    medal = kwargs['ti'].xcom_pull(task_ids='pick_medal')
    if medal == 'Gold':
        return 'calk_Gold'
    elif medal == 'Silver':
        return 'calk_Silver'
    else:
        return 'calk_Bronze'

def generate_delay():
    # time.sleep(35)
    time.sleep(random.randint(20, 40))


# Назва з'єднання з базою даних MySQL
connection_name = "mysql_course"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 12),
}

with DAG('medals_dag_final', 
         default_args=default_args, 
         schedule='*/3 * * * *', 
         catchup=False, 
         tags=['Oleksandr']
) as dag:

# Завдання для створення схеми бази даних (якщо не існує)
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS oleksandr_hw;
        """
    )    
# Завдання для створення таблиці "medals" (якщо не існує)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=connection_name,
        sql="""CREATE TABLE IF NOT EXISTS oleksandr_hw.medals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    medal_type VARCHAR(255) NOT NULL,
    count int NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
    )

# Завдання для генерації випадкової медалі
    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=generate_random_medal
    )

# Завдання для вибору наступного завдання на основі згенерованої медалі
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=calk_medal
    )

# Завдання для підрахунку кількості золотих медалей
    calk_gold = SQLExecuteQueryOperator(
        task_id='calk_Gold',
        conn_id=connection_name,
        sql="""
        INSERT INTO oleksandr_hw.medals (medal_type, count) 
        SELECT 'Gold', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Gold';
        """
    )

# Завдання для підрахунку кількості срібних медалей
    calk_silver = SQLExecuteQueryOperator(
        task_id='calk_Silver',
        conn_id=connection_name,
        sql="""
        INSERT INTO oleksandr_hw.medals (medal_type, count) 
        SELECT 'Silver', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Silver';
        """
    )

    calk_bronze = SQLExecuteQueryOperator(
        task_id='calk_Bronze',
        conn_id=connection_name,
        sql="""
        INSERT INTO oleksandr_hw.medals (medal_type, count) 
        SELECT 'Bronze', COUNT(*) 
        FROM olympic_dataset.athlete_event_results 
        WHERE medal = 'Bronze';
        """
    )

    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=tr.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT COUNT(*) 
        FROM oleksandr_hw.medals
        WHERE created_at = (SELECT MAX(created_at) FROM oleksandr_hw.medals)
        AND created_at >=NOW() - INTERVAL 30 SECOND;
        """,
        poke_interval=5,
        timeout=20,
        mode='poke'
    )




create_schema >> create_table >> pick_medal >> pick_medal_task
pick_medal_task >> [calk_gold, calk_silver, calk_bronze]
[calk_gold, calk_silver, calk_bronze] >> generate_delay_task
generate_delay_task >> check_for_correctness