"Airflow DAG to copy the date from one postgres instance to other"
import datetime
from datetime import timedelta
from sys import setcheckinterval
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def create_table(
    docker_service_name, database, user, password, port, schemaname, table_name
):
    conn_str = f"postgresql+psycopg2://{user}:{password}@{docker_service_name}:{port}/{database}"
    engine = create_engine(conn_str)
    connect = engine.connect()
    connect.execute(
        f"""
    DROP TABLE IF EXISTS {schemaname}.{table_name};
    CREATE TABLE {schemaname}.{table_name}
    (
        id INT PRIMARY KEY,
        creation_date DATE NOT NULL, 
        sale_value INT NOT NULL
    );
    commit;
    """
    )
    print(f"Table {schemaname}{table_name} created")
    connect.close()


def insert_table(
    docker_service_name, database, user, password, port, schemaname, table_name, data
):
    conn_str = f"postgresql+psycopg2://{user}:{password}@{docker_service_name}:{port}/{database}"
    engine = create_engine(conn_str)
    connect = engine.connect()
    for i in data:
        connect.execute(
            f"""
        INSERT INTO {schemaname}.{table_name} (id, creation_date, sale_value)
        VALUES {i};
        commit;
        """
        )
    print("Data inserted")
    connect.close()


def get_table(
    docker_service_name, database, user, password, port, schemaname, table_name
):
    conn_str = f"postgresql+psycopg2://{user}:{password}@{docker_service_name}:{port}/{database}"
    engine = create_engine(conn_str)
    connect = engine.connect()
    data = connect.execute(f"select * from {schemaname}.{table_name};")
    connect.close()
    data_upload = []
    for i in data:
        i = list(i)
        i[1] = i[1].strftime("%m-%d-%Y")
        i = tuple(i)
        data_upload.append(i)
    print(data_upload)
    return tuple(data_upload)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 11, 28),
    "email": ["gouravthakur0197@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    dag_id="glints",
    schedule_interval=None,
    description="Transfering data from one database to other",
    default_args=default_args,
)

start_task = DummyOperator(task_id="start_execution", dag=dag)
end_task = DummyOperator(task_id="end_execution", dag=dag)
create_tab1 = PythonOperator(
    task_id="Creating_table1",
    python_callable=create_table,
    op_args=["db", "postgres", "postgres", "postgres", 5432, "public", "sale"],
    dag=dag,
)
create_tab2 = PythonOperator(
    task_id="Creating_table2",
    python_callable=create_table,
    op_args=["postgres", "postgres", "airflow", "airflow", 5432, "public", "sale"],
    dag=dag,
)
insert_tab1 = PythonOperator(
    task_id="Insert_table1",
    python_callable=insert_table,
    op_args=[
        "postgres",
        "postgres",
        "airflow",
        "airflow",
        5432,
        "public",
        "sale",
        [(1, "12-12-2021", 1000), (2, "12-13-2021", 2000)],
    ],
    dag=dag,
)

data = get_table("db", "postgres", "postgres", "postgres", 5432, "public", "sale")


insert_tab2 = PythonOperator(
    task_id="Insert_table2",
    python_callable=insert_table,
    op_args=[
        "postgres",
        "postgres",
        "airflow",
        "airflow",
        5432,
        "public",
        "sale",
        data,
    ],
    dag=dag,
)
inspect = PythonOperator(
    task_id="Inpect_result",
    python_callable=get_table,
    op_args=["postgres", "postgres", "airflow", "airflow", 5432, "public", "sale"],
    dag=dag,
)
start_task >> create_tab1 >> create_tab2 >> insert_tab1 >> insert_tab2 >> inspect >> end_task
