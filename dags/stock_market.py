from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.hooks.base import BaseHook
from datetime import datetime

# import requests
# import pandas as pd
# from sqlalchemy import create_engine
# from airflow.models import Variable

# api = Variable.get("ALPHA_VANTAGE")

# from dotenv import load_dotenv
# import os

SYMBOL = "IBM"

def fetch_and_store_alpha_vantage():
        import requests
        import pandas as pd
        from sqlalchemy import create_engine
        from dotenv import load_dotenv
        import os
        
        load_dotenv()
        
        api = os.getenv("ALPHA_VANTAGE")

        # Fetch data
        
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={api}"
        response = requests.get(url)
        data = response.json()["Time Series (Daily)"]

        # Convert to DataFrame
        
        df = pd.DataFrame.from_dict(data, orient="index")
        df.index = pd.to_datetime(df.index)
        df = df.astype(float)
        df.reset_index(inplace=True)
        df.rename(columns={"index": "date"}, inplace=True)

        # Get Postgres connection from Airflow
        conn = BaseHook.get_connection("postgres_default")
        engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

        # Store into Postgres
        df.to_sql("alpha_vantage_daily", engine, if_exists="replace", index=False)
        print("Data stored in Postgres table: alpha_vantage_daily")
        
    

with DAG(
        dag_id="alpha_vantage_postgres_dag",
        start_date=datetime(2025, 1, 1),
        schedule="@daily",  # run daily
        catchup=False,
        tags=["alpha_vantage", "postgres"],
    ) as dag:

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store_alpha_vantage",
        python_callable=fetch_and_store_alpha_vantage,
    )

    fetch_and_store
