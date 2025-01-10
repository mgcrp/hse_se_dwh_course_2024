### План семинара

#### 1 - Advanced AirFlow pipelines

1) Активируем БД (используем базу из вашей первой ДЗ)<br>
`sh demo_db/docker-init.sh`

2) Активируем Airflow<br>
`cd demo_airflow && docker-compose up -d`

3) Добавим connection в AirFlow<br>
Тип подключения - PostgreSQL<br>
host: `host.docker.internal`<br>
port: `5432`<br>
user: `postgres`<br>
pass: `***`<br>
db: `postgres`<br>

4) Давайте напишем простой DAG с sqlOperator<br>
demo_airflow/dags/dag_demoSql.py<br>
Список параметров, которые мы можем подставлять через Jinja в AirFlow https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

5) DAG с sqlSensor <br>
demo_airflow/dags/dag_demoSqlSensor.py

6) DAG с pythonOperator<br>
    - Кладем токен в секреты<br>
        host: `https://api.telegram.org/bot`<br>
        user: `hse_dwh_course_bot`<br>
        pass: `***`
    - demo_airflow/dags/dag_demoPython.py

7) DAG с OnFailureCallback<br>
demo_airflow/dags/dag_demoOnFailureCallback.py

#### 2 - dbt

1) Ставим dbt<br>
```bash
pip3 install dbt
pip3 install dbt-postgres
```

2) Создаем проект<br>
`dbt init demo_dbt`

3) Настраиваем profiles.yml<br>
Он лежит либо в `/Users/<username>/.dbt/profiles.yml`, либо может быть переопределен в директории проекта<br>
Для референса смотрим https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup

4) Настраиваем dbt_project.yml<br>
    - Меняем profile (если у вас свой и вы не меняли профайл проекта в прошлом шаге)
    - Указываем схему для моделей в папке example:<br>
    `+schema: dbt_example`

5) Проверяем подключение<br>
`dbt debug`

6) Запускаем стандартные примеры<br>
`dbt run`

7) Добавим в систему source - таблицы из нашей БД<br>
```bash
cd models
mkdir raw
cd raw
touch schema.yml
```
```yaml
version: 2

sources:
  - name: postgres_business
    description: 'blah-blah-blah'
    database: postgres
    schema: business
    tables:
      - name: categories
        description: 'A table containing item categories'
        columns:
          - name: category_id
            description: 'INT, primary key'
          - name: ategory_name
            description: 'TEXT, human readable name'
      - name: customers
      - name: manufacturers
      - name: deliveries
      - name: price_change
      - name: products
      - name: purchase_items
      - name: purchases
      - name: stores
```
`dbt run`

8) В директории `/target/compiled` - сгенерированные файлы .sql без jinja

9) Нам не нравится, что оно пишет в кривую схему<br>
Как можно задать схему для модели:
    - В файле модели
      `{{ config(materialized='table', schema='staging') }}`
    - на уровне проекта (dbt_project.yml)
      `+schema: staging`
    - изобретаем макрос, чтобы сделать схему красивую
        - дефолтный макрос - https://docs.getdbt.com/docs/build/custom-schemas
        - `touch /macros/generate_schema_name.sql`
        - Содержание файла
```html
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
```
    - `dbt run`

10) Создадим новую таблицу
    - `cd /models`
    - `mkdir presentation`
    - `touch presentation__gmv_by_store_category.sql`
    - Содержание файла
```html
{{ config(materialized='table', schema='presentation') }}

with final as (
    SELECT
        pu.store_id,
        pr.category_id,
        SUM(pi.product_price * pi.product_count) AS sales_sum
    FROM
        {{ source('postgres_business', 'purchase_items') }} pi
    JOIN
        {{ source('postgres_business', 'products') }} pr ON pi.product_id = pr.product_id
    JOIN
        {{ source('postgres_business', 'purchases') }} pu ON pi.purchase_id = pu.purchase_id
    GROUP BY
        pu.store_id,
        pr.category_id
)

select * from final
    ```
    - обновляем dbt_project.yml
    ```
    presentation:
      +materialized: table
      +schema: dbt_presentation
```
    - `dbt run`

11) Можно писать и гонять тесты на колонках - `dbt test`
    - Какие тесты гонять - пишется в schema.yml
    - Примеры тестов есть в example
    - Скомпилированные тесты пишутся в /target/compiled/.../models/example/schema.yml

12) Материализации:
    - table
    - view
    - incremental
    - ephemeral
Дефолтный тип - view<br>
Можно менять на уровне проекта, можно в каждой таблице отдельно

13) `dbt seeds`
Cтатическая инфа, которая меняется редко
Кладется в формате csv в папку seeds в проекте
    - `cd seeds`
    - `touch dict_cities.csv`
    - Содержание файла
```csv
id, name, code
1, Moscow, MSK
2, Saint-Peterburg, SPB
3, Ekaterinburg, EKB
4, Novosibirsk, NSK
```
    - `dbt seed`

14) Автодока (только если вы писали описания для полей)
    - `dbt docs generate`
    - `dbt docs serve --port 1111`

15) Существует много сторонних packages с наборами макросов
    - https://hub.getdbt.com
    - touch packages.yml
    - Содержание файла
```
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```
    - dbt deps
    - Файлы packages лежат в директории dbt_packages

16) Пишем свой тест `test_is_moscow.sql`<br>
```html
{% macro test_is_moscow(model, column_name) %}

with validation as (
    select {{ column_name }} as ts
    from {{ model }}
),
validation_errors as (
    select ts
    from validation
    where 1=1
        and ts < '2023-01-01'
)
select count(*)
from validation_errors
```
    - Потом его можно включить в schema.yml (тест будет называться `is_moscow`)

#### 3 - Automate DV

1) Датасет - https://www.tpc.org/tpch/
2) Документация - https://automate-dv.readthedocs.io/en/v0.8.3/
3) Инит датасета на базе
```sql
create schema tpch;

CREATE TABLE tpch.customer
(C_CUSTKEY INT,
 C_NAME VARCHAR(25),
 C_ADDRESS VARCHAR(40),
 C_NATIONKEY INTEGER,
 C_PHONE CHAR(15),
 C_ACCTBAL DECIMAL(15,2),
 C_MKTSEGMENT CHAR(10),
 C_COMMENT VARCHAR(117));

CREATE TABLE tpch.lineitem
(L_ORDERKEY BIGINT,
 L_PARTKEY INT,
 L_SUPPKEY INT,
 L_LINENUMBER INTEGER,
 L_QUANTITY DECIMAL(15,2),
 L_EXTENDEDPRICE DECIMAL(15,2),
 L_DISCOUNT DECIMAL(15,2),
 L_TAX DECIMAL(15,2),
 L_RETURNFLAG CHAR(1),
 L_LINESTATUS CHAR(1),
 L_SHIPDATE DATE,
 L_COMMITDATE DATE,
 L_RECEIPTDATE DATE,
 L_SHIPINSTRUCT CHAR(25),
 L_SHIPMODE CHAR(10),
 L_COMMENT VARCHAR(44));

CREATE TABLE tpch.nation
(N_NATIONKEY INTEGER,
 N_NAME CHAR(25),
 N_REGIONKEY INTEGER,
 N_COMMENT VARCHAR(152));

CREATE TABLE tpch.orders
(O_ORDERKEY BIGINT,
 O_CUSTKEY INT,
 O_ORDERSTATUS CHAR(1),
 O_TOTALPRICE DECIMAL(15,2),
 O_ORDERDATE DATE,
 O_ORDERPRIORITY CHAR(15),
 O_CLERK  CHAR(15),
 O_SHIPPRIORITY INTEGER,
 O_COMMENT VARCHAR(79));

CREATE TABLE tpch.part
(P_PARTKEY INT,
 P_NAME VARCHAR(55),
 P_MFGR CHAR(25),
 P_BRAND CHAR(10),
 P_TYPE VARCHAR(25),
 P_SIZE INTEGER,
 P_CONTAINER CHAR(10),
 P_RETAILPRICE DECIMAL(15,2),
 P_COMMENT VARCHAR(23));

CREATE TABLE tpch.partsupp
(PS_PARTKEY INT,
 PS_SUPPKEY INT,
 PS_AVAILQTY INTEGER,
 PS_SUPPLYCOST DECIMAL(15,2),
 PS_COMMENT VARCHAR(199));

CREATE TABLE tpch.region
(R_REGIONKEY INTEGER,
 R_NAME CHAR(25),
 R_COMMENT VARCHAR(152));

CREATE TABLE tpch.supplier
(S_SUPPKEY INT,
 S_NAME CHAR(25),
 S_ADDRESS VARCHAR(40),
 S_NATIONKEY INTEGER,
 S_PHONE CHAR(15),
 S_ACCTBAL DECIMAL(15,2),
 S_COMMENT VARCHAR(101));

copy tpch.customer from  '/var/lib/postgresql/data/src/customer.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.lineitem from  '/var/lib/postgresql/data/src/lineitem.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.nation from  '/var/lib/postgresql/data/src/nation.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.orders from  '/var/lib/postgresql/data/src/orders.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.part from  '/var/lib/postgresql/data/src/part.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.partsupp from  '/var/lib/postgresql/data/src/partsupp.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.region from  '/var/lib/postgresql/data/src/region.csv' WITH (FORMAT csv, DELIMITER '|');
copy tpch.supplier from  '/var/lib/postgresql/data/src/supplier.csv' WITH (FORMAT csv, DELIMITER '|');
```
4)
```bash
dbt run -m tag:raw
dbt run -m tag:stage
dbt run -m tag:hub
dbt run -m tag:link
dbt run -m tag:satellite
dbt run -m tag:t_link
```
