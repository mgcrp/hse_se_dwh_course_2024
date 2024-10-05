## Семинар 2. Репликация, S3, Hadoop.

### Демо 1 - Ручная настройка репликации

1. Надо пропатчить конфиги
    - pg_hba.conf - сетевой конфиг базы; разрешим подключения с любых внешних хостов к любой БД на сервере под любым юзером с авторизацией по паролю (шифр scram-sha-256);
    ```yaml
    host    all             all             all                     scram-sha-256
    ```
    - postgresql.conf - настройки СУБД;
    ```bash
    #------------------------------------------------------------------------------
    # WRITE-AHEAD LOG
    #------------------------------------------------------------------------------
    # - Settings -
    wal_level = logical
    #------------------------------------------------------------------------------
    # REPLICATION
    #------------------------------------------------------------------------------
    # - Sending Servers -
    max_wal_senders = 2
    max_replication_slots = 2
    # - Standby Servers -
    hot_standby = on
    hot_standby_feedback = on
    ```
    тут:
        - **wal_level** указывает, сколько информации записывается в WAL (журнал операций, который используется для репликации).
        Значение `replica` указывает на необходимость записывать только данные для поддержки архивирования WAL и репликации.
        Значение `logical` дает возможность поднять полную логическую реплику, которая может заменить мастер в случае его падения;
        - **max_wal_senders** — количество планируемых слейвов; 
        - **max_replication_slots** — максимальное число слотов репликации; 
        - **hot_standby** — определяет, можно или нет подключаться к postgresql для выполнения запросов в процессе восстановления; 
        - **hot_standby_feedback** — определяет, будет или нет сервер slave сообщать мастеру о запросах, которые он выполняет.
    
2. Подложим новые конфиги в БД. Мы уже умеем так делать через docker volumes:
```yaml
volumes:
    - ./init-script/config/pg_hba.conf:/etc/postgresql/pg_hba.conf
    - ./init-script/config/postgres.conf:/etc/postgresql/postgresql.conf
```
3. Сделаем конфиг репликации для слейва:
    - pg_hba.conf
    ```yaml
    primary_conninfo='host=postgres_master port=5432 user=replicator password=my_replicator_password'
    primary_slot_name='replication_slot_slave1'
    ```
4. Запускаем master и консоль
`docker-compose up -d postgres_master`
`docker exec -it postgres_master bash`
5. Создаем пользователя под репликацию на мастере
```bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'my_replicator_password';
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_slave1');
EOSQL
```
6. Создаем бэкап мастера, из которого мы поднимем slave. Важно - к data-slave мы уже пробросили volume, значит он у нас появится на хосте, а потом мы сможем замаунтить на него slave;
```bash
pg_basebackup -D /var/lib/postgresql/data-slave -S replication_slot_slave1 -X stream -P -U replicator -Fp -R
```
7. Заменяем сетевой конфиг мастера и добавляем конфиг подключения к слоту репликации на слейве:
```bash
cp /etc/postgresql/init-script/slave-config/* /var/lib/postgresql/data-slave
cp /etc/postgresql/init-script/config/pg_hba.conf /var/lib/postgresql/data
```
8. Рестартим мастер
`docker-compose restart postgres_master`
9. Поднимаем slave
`docker-compose up -d postgres_slave`

### Демо 2 - Все то же самое, только автоматизированное

`sh docker-init.sh`

### Демо 3 - minio

См. `minio_demo.ipynb`

### Демо 4 - hadoop

1. `docker-compose up -d`
Важно: очень долго!
2. `docker cp archive.zip namenode:archive.zip`
3. `docker cp breweries.csv namenode:breweries.csv`
4. `docker exec -it namenode bash`
5. `hdfs dfsadmin -safemode leave`
6. `hdfs dfs -mkdir -p /data/mgcrp`
7. `hdfs dfs -ls /data`
8. `hdfs dfs -put archive.zip /data/mgcrp/archive.zip`
9. `hdfs fsck /data/mgcrp`
10. `hdfs dfs -put breweries.csv /data/mgcrp/breweries.csv`

1. `docker exec -it spark-master bash`
2. `/spark/bin/pyspark --master spark://spark-master:7077`
3. `spark`
4. `df = spark.read.csv('hdfs://namenode:9000/data/mgcrp/breweries.csv')`
5. `df.show()`
6. Spark
```python
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = spark.read \
    .option('header', 'true') \
    .csv('hdfs://namenode:9000/data/mgcrp/breweries.csv')

df.groupby('state') \
    .count() \
    .repartition(1) \
    .write \
    .mode('overwrite') \
    .option('header', 'true') \
    .csv('hdfs://namenode:9000/data/mgcrp/breweries_groupby_pySpark.csv')
```
7. MapReduce
```
docker cp archive.zip namenode:archive.zip
docker cp breweries.csv namenode:breweries.csv
docker cp mapper.py namenode:mapper.py
docker cp mapper_old.py namenode:mapper_old.py
docker cp reducer.py namenode:reducer.py
docker cp reducer_old.py namenode:reducer_old.py
docker cp pg4300.txt namenode:pg4300.txt
docker cp pg5000.txt namenode:pg5000.txt
```

```
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
-file mapper_old.py    -mapper mapper_old.py \
-file reducer_old.py   -reducer reducer_old.py \
-input /data/text/* -output /data/text-output
```


```
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-3.2.2.jar \
-file mapper.py    -mapper mapper.py \
-file reducer.py   -reducer reducer.py \
-input /data/text/* -output /data/text-output
```

```
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-3.2.2.jar \
-file mapper_old.py    -mapper mapper_old.py \
-file reducer_old.py   -reducer reducer_old.py \
-input /data/text/* -output /data/text-output2
```