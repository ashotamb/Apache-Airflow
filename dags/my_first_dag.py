from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Пример DAG для обработки данных',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['lab2', 'example'],
)

def generate_data(**context):
    data = [random.randint(1, 100) for _ in range(10)]
    print(f"Сгенерированные данные: {data}")
    context['ti'].xcom_push(key='raw_data', value=data)
    return data

def process_data(**context):
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='generate_data')
    filtered = [x for x in raw_data if x > 50]
    avg = sum(filtered) / len(filtered) if filtered else 0
    result = {
        'filtered_data': filtered,
        'count': len(filtered),
        'average': round(avg, 2),
        'max': max(filtered) if filtered else 0,
        'min': min(filtered) if filtered else 0,
    }
    print(f"Результат обработки: {result}")
    context['ti'].xcom_push(key='processed_data', value=result)
    return result

def save_results(**context):
    processed = context['ti'].xcom_pull(key='processed_data', task_ids='process_data')
    with open('/tmp/airflow_results.txt', 'w') as f:
        f.write("=== РЕЗУЛЬТАТЫ ОБРАБОТКИ ДАННЫХ ===\n")
        f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Отфильтрованные данные: {processed['filtered_data']}\n")
        f.write(f"Количество элементов: {processed['count']}\n")
        f.write(f"Среднее значение: {processed['average']}\n")
        f.write(f"Максимум: {processed['max']}\n")
        f.write(f"Минимум: {processed['min']}\n")
    print("Результаты сохранены в /tmp/airflow_results.txt")

task_generate = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    provide_context=True,
    dag=dag,
)

task_process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

task_save = PythonOperator(
    task_id='save_results',
    python_callable=save_results,
    provide_context=True,
    dag=dag,
)

task_notify = BashOperator(
    task_id='send_notification',
    bash_command='echo "Pipeline completed! Results: $(cat /tmp/airflow_results.txt)"',
    dag=dag,
)

task_generate >> task_process >> task_save >> task_notify