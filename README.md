# Forex Data Pipeline 

## Sobre o projeto

Este projeto tem como objetivo realizar alguns testes utilizando a ferramenta Apache Airflow utilizando alguns dos principais Operators que a ferramenta possui. 

Essa demonstração utiliza uma imagem docker contendo as seguintes ferramentas:

- Apache Airflow
- Hadoop
- Hive
- Hue
- Livy
- Postgress
- Apache Spark

## Inicializando os containers docker

Utilizar os comandos abaixo:

```sh
docker build -t hadoop-base docker/hadoop/hadoop-base
docker build -t hive-base docker/hive/hive-base
docker build -t spark-base docker/spark/spark-base
docker-compose up -d --build
```

Ou executar o arquivo start.sh:

```sh
./start.sh
```

## Finalizando os containers docker

Utilizar o comando abaixo: 

```
docker-compose down
```
Ou executar o arquivo stop.sh:

```sh
./stop.sh
```

## Tarefas executadas

O fluxo dessa pipeline consiste em: 

1 - Verificar se o link está disponível
2 - Verificar se o arquivo está disponível
3 - Realizar o download do arquivo
4 - Salvar o arquivo em HDFS
5 - Criar tabela em Hive
6 - Popular a tabela com os dados do arquivo utilizando spark
7 - Enviar notificação de e-mail 

Para enviar a notificação de e-mail o arquivo **mnt\airflow\airflow.cfg** precisa ser configurado: 

```Python
# If you want airflow to send emails on retries, failure, and you want to use
# the airflow.utils.email.send_email_smtp function, you have to configure an
# smtp server here
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
# Example: smtp_user = airflow
smtp_user = seu_email@gmail.com
# Example: smtp_password = airflow
smtp_password = sua_senha
smtp_port = 587
smtp_mail_from = seu_email@gmail.com
smtp_timeout = 30
smtp_retry_limit = 5
```

Os operators utilizados foram 

- HttpSensor
- FileSensor
- PythonOperator
- BashOperator
- HiveOperator
- SparkSubmitOperator
- EmailOperator

## Conexões necessárias

No diretório **docs** foi criado o arquivo **connections.txt** contendo todas as conexões que são necessárias. Essas conexões precisam ser informadas conforme imagem abaixo:

img1 aqui

## Fluxo da DAG

O fluxo criado pode ser observado conforme figura abaixo: 

img2 

## Fonte

A fonte deste conteúdo é baseada no curso https://www.udemy.com/course/the-ultimate-hands-on-course-to-master-apache-airflow/ do instrutor Marc Lamberti no qual aborda de maneira aprofundada o Apache Airflow. 




