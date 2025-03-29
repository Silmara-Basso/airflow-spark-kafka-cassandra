
import sys
import logging
import json
import re
import argparse
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, instr
from pyspark.sql.types import StructType, StructField, StringType

# Configura o logger para exibir mensagens de log com nível INFO
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Função para criar um keyspace no Cassandra
def sil_cria_keyspace(session):

    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS dados_usuarios
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )

    print("Keyspace criada com sucesso!")

# Função para criar uma tabela no Cassandra
def sil_cria_tabela(session):

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS dados_usuarios.tb_usuarios (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """
    )

    print("Table criada com sucesso!")

# Função para formatar strings antes de inseri-las no Cassandra
def sil_formata_string_cassandra(text: str):
    return re.sub(r"'", r"''", text)

# Função para inserir dados formatados no Cassandra
def sil_insere_dados(session, row):

    # Formata e extrai campos do registro recebido
    user_id         = sil_formata_string_cassandra(row.id)
    first_name      = sil_formata_string_cassandra(row.first_name)
    last_name       = sil_formata_string_cassandra(row.last_name)
    email           = sil_formata_string_cassandra(row.email)
    username        = sil_formata_string_cassandra(row.username)
    gender          = sil_formata_string_cassandra(row.gender)
    address         = sil_formata_string_cassandra(row.address)
    post_code       = sil_formata_string_cassandra(row.post_code)
    dob             = sil_formata_string_cassandra(row.dob)
    registered_date = sil_formata_string_cassandra(row.registered_date)
    phone           = sil_formata_string_cassandra(row.phone)
    picture         = sil_formata_string_cassandra(row.picture)

    try:
        # Cria a query
        query = f"""
            INSERT INTO dados_usuarios.tb_usuarios(
                id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture
            ) VALUES (
                '{user_id}', '{first_name}', '{last_name}', '{gender}', '{address}', '{post_code}', 
                '{email}', '{username}', '{dob}', '{registered_date}', '{phone}', '{picture}'
            )
        """
        
        # Insere dados formatados na tabela do Cassandra
        session.execute(query)
        logging.info(f" Dados inseridos para o registro: {user_id} - {first_name} {last_name} - {email}")
    except Exception as e:
        # Exibe erro no caso de falha ao inserir os dados
        logging.error(f"Os dados não podem ser inseridos devido ao erro: {e}")
        print(f"Log - Esta é a query:\n{query}")

# Função para criar uma conexão Spark
def sil_cria_spark_connection():

    try:
        # Configura e cria a conexão com o Spark
        s_conn = (
            SparkSession.builder.appName("silProjeto")
            .master("spark://spark-master:7077")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.executor.memory", "1g")
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "2")
            .getOrCreate()
        )

        # Define o nível de log
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark Connection criada com sucesso!")
        return s_conn
    except Exception as e:
        logging.error(f"Não foi possível criar a Spark Connection devido ao erro: {e}")
        return None

# Função para criar uma conexão com o Kafka no Spark
def sil_cria_kafka_connection(spark_conn, stream_mode):

    try:
        # Configura e cria um DataFrame Spark para leitura de dados do Kafka
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "sil_kafka_topic")
            .option("startingOffsets", stream_mode)
            .load()
        )
        logging.info("Dataframe Kafka criado com sucesso")
        return spark_df
    except Exception as e:
        # Exibe um aviso no caso de falha ao criar o DataFrame Kafka
        logging.warning(f"O dataframe Kafka não pôde ser criado devido ao erro: {e}")
        return None

# Função para criar um DataFrame estruturado a partir dos dados do Kafka
def sil_cria_df_from_kafka(spark_df):

    # Define o esquema dos dados recebidos no formato JSON
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    # Processa os dados do Kafka para extrair e filtrar os registros
    return (
        spark_df.selectExpr("CAST(value AS STRING)")            # Converte os dados para string
        .select(from_json(col("value"), schema).alias("data"))  # Converte JSON para colunas estruturadas
        .select("data.*")                                       # Extrai todas as colunas do campo "data"
        .filter(instr(col("email"), "@") > 0)                   # Filtra e retorna somente registros onde o email contém '@'
    )

# Função para criar uma conexão com o Cassandra
def sil_cria_cassandra_connection():

    try:
        # Cria um cluster e retorna a sessão de conexão com o Cassandra
        cluster = Cluster(["cassandra"])
        return cluster.connect()
    except Exception as e:
        # Exibe um erro no caso de falha ao conectar ao Cassandra
        logging.error(f"Não foi possível criar a conexão Cassandra devido ao erro: {e}")
        return None

# Ponto de entrada principal do programa
if __name__ == "__main__":
    
    # Configura o parser para argumentos de linha de comando
    parser = argparse.ArgumentParser(description = "Real Time ETL.")
    
    # Adiciona o argumento para o modo de consumo dos dados
    parser.add_argument(
        "--mode",
        required=True,
        help="Modo de consumo dos dados",
        choices=["initial", "append"],
        default="append",
    )

    # Analisa os argumentos fornecidos
    args = parser.parse_args()

    # Define o modo de consumo com base no argumento fornecido
    stream_mode = "earliest" if args.mode == "initial" else "latest"

    # Cria conexões com o Cassandra e Spark
    session = sil_cria_cassandra_connection()
    spark_conn = sil_cria_spark_connection()

    # Se tiver sessão criada
    if session and spark_conn:

        # Cria o keyspace e a tabela no Cassandra
        sil_cria_keyspace(session)
        sil_cria_tabela(session)

        # Cria uma conexão com o Kafka e obtém um DataFrame
        kafka_df = sil_cria_kafka_connection(spark_conn, stream_mode)

        if kafka_df:

            # Cria um DataFrame estruturado a partir dos dados do Kafka
            structured_df = sil_cria_df_from_kafka(kafka_df)

            # Função para processar lotes de dados
            def process_batch(batch_df, batch_id):
                
                # Itera sobre as linhas do lote e insere os dados no Cassandra
                for row in batch_df.collect():
                    sil_insere_dados(session, row)

            # Configura o processamento contínuo do DataFrame estruturado
            query = (
                structured_df.writeStream
                .foreachBatch(process_batch)  # Define o processamento por lote
                .start()                      # Inicia o processamento
            )
        
            # Aguarda a conclusão do fluxo
            query.awaitTermination()

