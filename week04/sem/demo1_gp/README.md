## Семинар 4. MPP на примере GreenPlum

1) Используемый контейнер

Ссылка на DockerHub - https://hub.docker.com/r/as2sp/greenplum7 <br>
Его конфигурация на GitHub - https://github.com/as2sp/docker-greenplum7 <br>
Внутри уже поднимается мастер и 4 сегмента внутри одного контейнера. Так делать неправильно, но исключительно для демо за неимением лучшего - пойдет.

2) Запускаем GreenPlum
```bash
docker run --name greenplum -p 5432:5432 -d as2sp/greenplum7
```

3) Используемые данные - tpch
https://www.tpc.org/tpch/

3.1) Создаем структуру БД в нашем GreenPlum
```sql
CREATE DATABASE tpch;

CREATE TABLE customer
(C_CUSTKEY INT, 
C_NAME VARCHAR(25),
C_ADDRESS VARCHAR(40),
C_NATIONKEY INTEGER,
C_PHONE CHAR(15),
C_ACCTBAL DECIMAL(15,2),
C_MKTSEGMENT CHAR(10),
C_COMMENT VARCHAR(117))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (C_CUSTKEY);

CREATE TABLE lineitem
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
L_COMMENT VARCHAR(44))
WITH (appendonly=true, orientation=column, compresstype=ZSTD)
DISTRIBUTED BY (L_ORDERKEY,L_LINENUMBER)
PARTITION BY RANGE (L_SHIPDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

CREATE TABLE nation
(N_NATIONKEY INTEGER, 
N_NAME CHAR(25), 
N_REGIONKEY INTEGER, 
N_COMMENT VARCHAR(152))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (N_NATIONKEY);

CREATE TABLE orders
(O_ORDERKEY BIGINT,
O_CUSTKEY INT,
O_ORDERSTATUS CHAR(1),
O_TOTALPRICE DECIMAL(15,2),
O_ORDERDATE DATE,
O_ORDERPRIORITY CHAR(15), 
O_CLERK  CHAR(15), 
O_SHIPPRIORITY INTEGER,
O_COMMENT VARCHAR(79))
WITH (appendonly=true, orientation=column, compresstype=ZSTD)
DISTRIBUTED BY (O_ORDERKEY)
PARTITION BY RANGE (O_ORDERDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

CREATE TABLE part
(P_PARTKEY INT,
P_NAME VARCHAR(55),
P_MFGR CHAR(25),
P_BRAND CHAR(10),
P_TYPE VARCHAR(25),
P_SIZE INTEGER,
P_CONTAINER CHAR(10),
P_RETAILPRICE DECIMAL(15,2),
P_COMMENT VARCHAR(23))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (P_PARTKEY);

CREATE TABLE partsupp
(PS_PARTKEY INT,
PS_SUPPKEY INT,
PS_AVAILQTY INTEGER,
PS_SUPPLYCOST DECIMAL(15,2),
PS_COMMENT VARCHAR(199))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (PS_PARTKEY,PS_SUPPKEY);

CREATE TABLE region
(R_REGIONKEY INTEGER, 
R_NAME CHAR(25),
R_COMMENT VARCHAR(152))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (R_REGIONKEY);

CREATE TABLE supplier 
(S_SUPPKEY INT,
S_NAME CHAR(25),
S_ADDRESS VARCHAR(40),
S_NATIONKEY INTEGER,
S_PHONE CHAR(15),
S_ACCTBAL DECIMAL(15,2),
S_COMMENT VARCHAR(101))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (S_SUPPKEY);
```

3.2) Генерируем датасет (для этого потребуются Make и gcc)

- Клонируем https://github.com/yuan-fei/TPCH-Greenplum
- Переходим в tpch/dbgen
- Модифицируем Makefile
    - https://github.com/yuan-fei/TPCH-Greenplum/blob/master/tpch/dbgen/Makefile#L103 <br>
    Устанавливаем `CC = clang`
    - https://github.com/yuan-fei/TPCH-Greenplum/blob/master/tpch/dbgen/Makefile#L109-L111 <br>
    Меняем при необходимости (например, если собираете проект под Windows, а не под Linux)
- В проекте используется старый стандарт языка C, поэтому может потребоваться модифицировать следующие файлы
    - https://github.com/yuan-fei/TPCH-Greenplum/blob/master/tpch/dbgen/bm_utils.c#L71 <br>
    `#include <malloc.h>` -> `#include <sys/malloc.h>`
    - https://github.com/yuan-fei/TPCH-Greenplum/blob/master/tpch/dbgen/varsub.c#L44 <br>
    `#include <malloc.h>` -> `#include <sys/malloc.h>`
- Если все пошло успешно - то эта команда выдаст справку по функции dbgen
```bash
./dbgen -h
```

- Далее можно сгенерить датасет - в моем случае, я сгенерил 10 Гб данных
```bash
./dbgen -vf -s 10
```

- Переводим датасет в csv формат для загрузки в GreenPlum
```bash
for i in `ls *.tbl`; do sed 's/|$//' $i > ${i/tbl/csv}; echo $i; done;
```

3.3) Загружаем полученные данные в GreenPlum; Для выполнения этой команды у вас должен быть установлен psql (идет в комплекте с postgresql, устанавливается `apt-get postgresql` на Ubuntu; на mac/arch можно установить с помощью brew или yum соответственно)

```bash
export GREENPLUM_URI="postgres://gpadmin:gppass@localhost:5432/tpch"
psql $GREENPLUM_URI

\copy "region"     from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/region.csv'        DELIMITER '|' CSV;
\copy "nation"     from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/nation.csv'        DELIMITER '|' CSV;
\copy "customer"   from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/customer.csv'      DELIMITER '|' CSV;
\copy "supplier"   from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/supplier.csv'      DELIMITER '|' CSV;
\copy "part"       from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/part.csv'          DELIMITER '|' CSV;
\copy "partsupp"   from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/partsupp.csv'      DELIMITER '|' CSV;
\copy "orders"     from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/orders.csv'        DELIMITER '|' CSV;
\copy "lineitem"   from '/{your github path}/GitHub/TPCH-Greenplum/tpch/dbgen/lineitem.csv'      DELIMITER '|' CSV;
```

4) Скрипты, которые я показывал на семинаре - `demo.sql`
