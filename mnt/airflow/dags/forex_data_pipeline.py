from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta
import csv
import requests
import json

#Criando dicionário com os parametros default
default_args = {
    "owner": "Afonso",
    "email_on_failure": False,
    "email_on_retry": False, 
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

#Função para realizar o download dos arquivos
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

#Especificando o nome da pipeline, data de início, intervalo de execução, argumentos default e catchup
with DAG("forex_data_pipeline", start_date=datetime(2022, 10, 12), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    #Criando um sensor para verificar a url
    is_forex_rates_available = HttpSensor(

        task_id="is_forex_rates_available", 
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20

    )

    #Criando um sensor para avaliar disponibilidade do arquivo
    is_forex_currencies_file_available = FileSensor(

        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20

    )

    #Realizando download do arquivo json
    downloading_rates = PythonOperator(

        task_id="downloading_rates",
        python_callable=download_rates
    )

    #Salvando arquivo em hdfs
    saving_rates = BashOperator(

        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """

    )

    #Criando tabela em Hive
    creating_forex_rates_tables = HiveOperator(

        task_id="creating_forex_rates_tables",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

    )

    #Inserindo o conteúdo do arquivo em tabela hive utilizand o script spark para processamento
    forex_processing = SparkSubmitOperator(

        task_id="forex_processing",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False

    )

    #Enviando e-mail de notificação
    send_email_notification = EmailOperator(

        task_id="send_email_notification", 
        to="email_para_receber_notificacao@gmail.com",
        subject="forex_data_pipeline",
        html_content="<h3>O pipeline forex_data_pipeline foi concluído com sucesso.</h3>"

    )


    #Orquestrando as tasks
    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates 
    downloading_rates >> saving_rates >> creating_forex_rates_tables 
    creating_forex_rates_tables >> forex_processing >> send_email_notification



    







