# airflow-spark-kafka-cassandra
Integração Apache Airflow, Apache Spark e Apache Kafka para criar uma plataforma open-source de dados, desde extração de dados, passando pelo processamento e armazenamento, até o agendamento e monitoramento de Jobs. Usaremos ainda o Apache Cassandra para armazenamento dos dados



## Servidor

Abra o terminal ou prompt de comando, navegue até a pasta com os arquivos (servivor) e execute:

`docker compose up -d`

o comando abaixo pode ser usado para desligar todos os containers:

`docker compose down`

Se fizer alguma alteração no arquivo yml o comando abaixo pode ser executado para recriar somente a imagem necessária:

`docker compose up --build -d`


## Cliente

Abra o terminal ou prompt de comando, navegue até a pasta com os arquivos (cliene) e execute o comando abaixo para criar a imagem:

`docker build -t kafka-spark-cassandra-consumer .`

Execute o comando abaixo para criar o container:

`docker run --name cliente -dit --network servidor_silnet kafka-spark-cassandra-consumer`

Dentro do container execute o comando abaixo para iniciar o Kafka Consumer:

`python sil_consumer_stream.py --mode initial`

Abra o terminal ou prompt de comando e use os comandos abaixo para acessar o Cassandra e verificar o resultado do armazenamento:

`docker exec -it cassandra cqlsh`
`USE dados_usuarios;`
`SELECT * FROM tb_usuarios;`

# Nota: No final do Dockerfile do cliente descomentte a última linha e recrie imagem e container para deixar o processo automático.
`CMD ["python", "sil_consumer_stream.py", "--mode", "append"]`