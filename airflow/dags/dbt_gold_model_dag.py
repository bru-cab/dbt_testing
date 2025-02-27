from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'dbt_complete_pipeline',
    default_args=default_args,
    description='A complete DAG for dbt pipeline: seeds → staging → core models → gold',
    schedule_interval='0 1 * * *',  # Run at 1 AM every day
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task 1: Run dbt seed
run_dbt_seeds = BashOperator(
    task_id='run_dbt_seeds',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt seed',
    dag=dag,
)

# Tasks for staging models
run_stg_customers = BashOperator(
    task_id='run_stg_customers',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models staging.stg_customers',
    dag=dag,
)

run_stg_orders = BashOperator(
    task_id='run_stg_orders',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models staging.stg_orders',
    dag=dag,
)

run_stg_payments = BashOperator(
    task_id='run_stg_payments',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models staging.stg_payments',
    dag=dag,
)

run_stg_products = BashOperator(
    task_id='run_stg_products',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models staging.stg_products',
    dag=dag,
)

# Tasks for core models
run_customers = BashOperator(
    task_id='run_customers',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models customers',
    dag=dag,
)

run_orders = BashOperator(
    task_id='run_orders',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models orders',
    dag=dag,
)

run_products = BashOperator(
    task_id='run_products',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models products',
    dag=dag,
)

# Task for gold model
run_dbt_gold_model = BashOperator(
    task_id='run_dbt_gold_model',
    bash_command='cd /Users/bruno/dbt_testing/jaffle_shop && dbt run --models gold',
    dag=dag,
)

# Define the task dependencies

# Seeds must run before staging models
run_dbt_seeds >> [run_stg_customers, run_stg_orders, run_stg_payments, run_stg_products]

# Staging models must complete before core models with correct dependencies
# Customers depends on stg_customers, stg_orders, and stg_payments
[run_stg_customers, run_stg_orders, run_stg_payments] >> run_customers

# Orders depends on stg_orders and stg_payments
[run_stg_orders, run_stg_payments] >> run_orders

# Products depends only on stg_products
run_stg_products >> run_products

# Core models must complete before gold model
[run_customers, run_orders, run_products] >> run_dbt_gold_model