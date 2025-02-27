import pandas as pd
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

CSV_PATH = "/Users/bruno/dbt_testing/jaffle_shop/seeds/raw_orders.csv"

# Function to append new records
def append_new_orders():
    try:
        existing_data = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        existing_data = pd.DataFrame(columns=["id", "user_id", "order_date", "product_id", "status"])

    # Get the last ID and generate new ones
    last_id = existing_data["id"].max() if not existing_data.empty else 0
    new_orders = []

    for i in range(5):  # Add 5 new orders per run
        new_orders.append({
            "id": last_id + i + 1,
            "user_id": random.randint(1, 100),
            "order_date": (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
            "product_id": random.randint(1, 20),
            "status": random.choice(["shipped", "processing", "delivered", "canceled"])
        })

    new_orders_df = pd.DataFrame(new_orders)
    updated_data = pd.concat([existing_data, new_orders_df], ignore_index=True)
    
    updated_data.to_csv(CSV_PATH, index=False)
    print(f"Added {len(new_orders)} new orders to {CSV_PATH}")

# Define the DAG
default_args = {
    "owner": "bruno",
    "start_date": days_ago(1),
    "retries": 1,
    "depends_on_past": False,
}

with DAG(
    "populate_raw_orders_csv",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily
    catchup=False,
    description="DAG to populate raw orders CSV file with random data",
    tags=["data_generation"],
) as dag:

    generate_orders = PythonOperator(
        task_id="generate_orders",
        python_callable=append_new_orders,
        dag=dag,
    )

    generate_orders