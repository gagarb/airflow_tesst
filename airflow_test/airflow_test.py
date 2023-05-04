"""
* Description:
    Este proceso tiene como finalidad realizar prueba técnica para TalentPitch
    de acuerdo a las directrices, se crean 3 funciones ejecutadas por medio de
    PythonOperator
"""
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import pandas as pd
import requests

# -------- Variables definition ------------------------
in_path = os.path.join(os.path.dirname(__file__), 'input')
file_name = 'Traffic_Flow_Map_Volumes.csv'
output_file_name = 'Traffic_Flow_Map_Volumes_alter.csv'
out_path = os.path.join(os.path.dirname(__file__), 'output')
url = Variable.get("url_test")


# -------- Function definition ------------------------
def input_file(url):
    """Descarga archivo a local de acuerdo a url determinada."""
    response = requests.get(url)
    inpunt_file_name = os.path.join(in_path, file_name)
    print(f'\n\n {response.status_code} \n\n')
    print(f'\n\n {inpunt_file_name} \n\n')
    if response.status_code == 200:
        with open(inpunt_file_name, "wb") as file:
            file.write(response.content)
        return '\n\n Done input file \n\n'
    else:
        return '\n\n Don´t input file \n\n'


def output_file(in_p, out_p):
    """Trandforma el archivo importado."""
    path_file = os.path.join(in_p, file_name)
    out_path_file = os.path.join(out_p, output_file_name)
    file_df = pd.read_csv(path_file, sep=',', header=0)
    print(f'\n\n {file_df.head(10)} \n\n')
    file_df = file_df.dropna(how='any').reset_index(drop=True)
    file_df = file_df.iloc[:, [0, 1, 2, 3]]
    print(f'\n\n {file_df.head(10)} \n\n')
    file_df.to_csv(out_path_file, index=False, sep=';')
    return '\n\n Done output file \n\n'


def reader_file(out_p):
    """Realiza operaciones sobre el archivo transformado."""
    out_path_file = os.path.join(out_p, output_file_name)
    file_df = pd.read_csv(out_path_file, sep=';', header=0)
    conteo = file_df.groupby(['YEAR']).size().reset_index(name='COUNT')
    print(f'\n\n {file_df.info()} \n\n')
    return f'\n\n {conteo} \n\n'


# -------- Dag definition ------------------------
default_args = {
    'owner': 'gustavo8704@hotmail.com',
    'depends_on_past': False,
    'retries': 0
}

dag = DAG('airflow_test',
          max_active_runs=1,
          schedule_interval='0 0 * * *',
          start_date=datetime(2022, 1, 13),
          tags=['1.input_file', '2.output_file', '3.reader_file'],
          default_args=default_args,
          catchup=False
          )

with dag:
    import_file_task = PythonOperator(
        task_id='import_file_task',
        python_callable=input_file,
        op_args=[url]
        )
    output_file_task = PythonOperator(
        task_id='output_file_task',
        python_callable=output_file,
        op_args=[in_path, out_path]
        )
    reader_file_task = PythonOperator(
        task_id='reader_file_task',
        python_callable=reader_file,
        op_args=[out_path]
        )
    import_file_task
    output_file_task.set_upstream(import_file_task)
    reader_file_task.set_upstream(output_file_task)
