
# Importa o módulo `uuid` para gerar identificadores únicos universais
import uuid

# Importa classes relacionadas a data e hora
from datetime import datetime, timedelta

# Importa as classes principais do Airflow para criar DAGs
from airflow import DAG

# Importa o operador Python do Airflow para execução de funções Python como tarefas
from airflow.operators.python import PythonOperator

# Define os argumentos padrão da DAG, incluindo o proprietário e a data de início
default_args = {"owner": "Silmara",
                "start_date": datetime(2025, 1, 9, 8, 10)}

# Define a função que obtém dados de uma API
def sil_extrai_dados_api():

    # Importa o módulo `requests` para fazer requisições HTTP
    import requests

    # Faz uma requisição GET para obter dados de uma API de usuários aleatórios
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]
    return res

# Define a função que formata os dados obtidos da API
def sil_formata_dados(res):

    data = {}
    location = res["location"]
    data["id"] = uuid.uuid4().hex
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"
    )
    data["post_code"] = location["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]

    return data

# Define a função que faz o streaming de dados para o Kafka
def sil_stream_dados():

    import json
    from kafka import KafkaProducer
    import time
    import logging

    try:

        # Cria uma conexão com o broker Kafka
        producer = KafkaProducer(bootstrap_servers = ["broker:29092"], max_block_ms = 5000)
        time.sleep(5)
        logging.info("Produtor Kafka conectado com sucesso.")

    except Exception as e:

        logging.error(f"Falha ao conectar ao corretor Kafka: {e}")
        return

    curr_time = time.time()

    # Executa o loop de streaming por 60 segundos
    while True:

        # Verifica se 60 segundos já se passaram
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:

            res = sil_extrai_dados_api()
            res = sil_formata_dados(res)

            # Envia os dados para o tópico Kafka
            producer.send("sil_kafka_topic", json.dumps(res).encode("utf-8"))

        except Exception as e:

            logging.error(f"Um erro ocorreu: {e}")
            continue

# Define a DAG do Airflow
with DAG("real-time-etl-stack",
         default_args=default_args,
         schedule=timedelta(days=1),
         catchup=False,
) as dag:
    # Define a tarefa que faz o streaming de dados
    streaming_task = PythonOperator(task_id="sil_stream_from_api", 
                                    python_callable=sil_stream_dados)




