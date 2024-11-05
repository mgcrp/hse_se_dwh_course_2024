/* создаем БД под TPC-H */

CREATE TABLE customer (
  C_CUSTKEY INT, 
  C_NAME VARCHAR(25),
  C_ADDRESS VARCHAR(40),
  C_NATIONKEY INTEGER,
  C_PHONE CHAR(15),
  C_ACCTBAL DECIMAL(15,2),
  C_MKTSEGMENT CHAR(10),
  C_COMMENT VARCHAR(117)
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (C_CUSTKEY);

CREATE TABLE lineitem (
  L_ORDERKEY BIGINT,
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
  L_COMMENT VARCHAR(44)
)
WITH (appendonly=true, orientation=column, compresstype=ZSTD)
DISTRIBUTED BY (L_ORDERKEY,L_LINENUMBER)
PARTITION BY RANGE (L_SHIPDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

CREATE TABLE nation (
  N_NATIONKEY INTEGER, 
  N_NAME CHAR(25), 
  N_REGIONKEY INTEGER, 
  N_COMMENT VARCHAR(152)
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (N_NATIONKEY);

CREATE TABLE orders (
  O_ORDERKEY BIGINT,
  O_CUSTKEY INT,
  O_ORDERSTATUS CHAR(1),
  O_TOTALPRICE DECIMAL(15,2),
  O_ORDERDATE DATE,
  O_ORDERPRIORITY CHAR(15), 
  O_CLERK  CHAR(15), 
  O_SHIPPRIORITY INTEGER,
  O_COMMENT VARCHAR(79)
)
WITH (appendonly=true, orientation=column, compresstype=ZSTD)
DISTRIBUTED BY (O_ORDERKEY)
PARTITION BY RANGE (O_ORDERDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

CREATE TABLE part (
  P_PARTKEY INT,
  P_NAME VARCHAR(55),
  P_MFGR CHAR(25),
  P_BRAND CHAR(10),
  P_TYPE VARCHAR(25),
  P_SIZE INTEGER,
  P_CONTAINER CHAR(10),
  P_RETAILPRICE DECIMAL(15,2),
  P_COMMENT VARCHAR(23)
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (P_PARTKEY);

CREATE TABLE partsupp (
  PS_PARTKEY INT,
  PS_SUPPKEY INT,
  PS_AVAILQTY INTEGER,
  PS_SUPPLYCOST DECIMAL(15,2),
  PS_COMMENT VARCHAR(199)
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (PS_PARTKEY,PS_SUPPKEY);

CREATE TABLE region (
  R_REGIONKEY INTEGER, 
  R_NAME CHAR(25),
  R_COMMENT VARCHAR(152)
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (R_REGIONKEY);

CREATE TABLE supplier (
  S_SUPPKEY INT,
  S_NAME CHAR(25),
  S_ADDRESS VARCHAR(40),
  S_NATIONKEY INTEGER,
  S_PHONE CHAR(15),
  S_ACCTBAL DECIMAL(15,2),
  S_COMMENT VARCHAR(101)
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (S_SUPPKEY);

---

/* Как посмотреть на колонки и их типы через information_schema */

select
	column_name,
  case
  	when data_type in ('character', 'character varying')
    	then data_type || '(' || character_maximum_length::character varying(7) || ')'
    else data_type
  end
from information_schema.columns
where 1=1
	and table_schema = 'public'
order by ordinal_position;

---

/* Как посмотреть, если ли перекосы в распределении данных по нодам */

/* из доки GreenPlum */
SELECT
	'Example Table' AS "Table Name",
	max(c) AS "Max Seg Rows",
  min(c) AS "Min Seg Rows",
	(max(c) - min(c)) * 100.0 / max(c) AS "Percentage Difference Between Max & Min"
FROM (SELECT count(*) c, gp_segment_id FROM public.customer GROUP BY 2) AS a;

/* из практики - часто используем на проде */
SELECT
	gp_segment_id ,
  count_on_segment ,
  (count_on_segment / sum(count_on_segment) OVER ()) * 100 AS precent_on_segment
FROM (
  SELECT
		gp_segment_id,
  	count(*) AS count_on_segment
	FROM public.customer
	GROUP BY gp_segment_id
) q
ORDER BY gp_segment_id;

/* Видно, что данные раскиданы сейчас +/- равномерно */

---

/* Давайте намеренно это сломаем и посмотрим, как появляются перекосы */

create schema seminar_demo;

/* Посмотрим, что c_nationkey - низкокардинальное поле */

select count(*), count(distinct c_nationkey)
from public.customer;

/* Создадим копию таблицы customer, только с сегментацией по низкокардинальному полю */

drop table if exists seminar_demo.customer;
create table seminar_demo.customer as
select * from public.customer
distributed by (c_nationkey);

/* Убедимся, что теперь данные распределены по нодам неравномерно */

SELECT
	'Example Table' AS "Table Name",
	max(c) AS "Max Seg Rows",
  min(c) AS "Min Seg Rows",
	(max(c) - min(c)) * 100.0 / max(c) AS "Percentage Difference Between Max & Min"
FROM (SELECT count(*) c, gp_segment_id FROM seminar_demo.customer GROUP BY 2) AS a;

SELECT
	gp_segment_id ,
  count_on_segment ,
  (count_on_segment / sum(count_on_segment) OVER ()) * 100 AS precent_on_segment
FROM (
  SELECT
		gp_segment_id,
  	count(*) AS count_on_segment
	FROM seminar_demo.customer
	GROUP BY gp_segment_id
) q
ORDER BY gp_segment_id;

---

/* Посмотрим, как различаются AO column и AO row таблицы */

drop table if exists seminar_demo.customer_AO_column;
create table seminar_demo.customer_AO_column (
	C_CUSTKEY INT, 
  C_NAME VARCHAR(25),
  C_ADDRESS VARCHAR(40),
  C_NATIONKEY INTEGER,
  C_PHONE CHAR(15),
  C_ACCTBAL DECIMAL(15,2),
  C_MKTSEGMENT CHAR(10),
  C_COMMENT VARCHAR(117)
)
WITH (APPENDONLY=True, COMPRESSLEVEL=5, ORIENTATION=COLUMN, COMPRESSTYPE=zlib)
DISTRIBUTED BY (C_CUSTKEY);

insert into seminar_demo.customer_AO_column
select * from public.customer;

drop table if exists seminar_demo.customer_AO_row;
create table seminar_demo.customer_AO_row (
	C_CUSTKEY INT, 
  C_NAME VARCHAR(25),
  C_ADDRESS VARCHAR(40),
  C_NATIONKEY INTEGER,
  C_PHONE CHAR(15),
  C_ACCTBAL DECIMAL(15,2),
  C_MKTSEGMENT CHAR(10),
  C_COMMENT VARCHAR(117)
)
WITH (APPENDONLY=True, COMPRESSLEVEL=5, ORIENTATION=ROW, COMPRESSTYPE=zlib)
DISTRIBUTED BY (C_CUSTKEY);

insert into seminar_demo.customer_AO_row
select * from public.customer;

select 
	sotdoid
	, sotdschemaname
	, sotdtablename
	, sotdsize
	, sotdtoastsize
	, sotdadditionalsize
	, pg_size_pretty(sotdsize) sotdsize_pretty 
	, pg_size_pretty(sotdtoastsize) sotdtoastsize_pretty
	, pg_size_pretty(sotdadditionalsize) sotdadditionalsize_pretty
from gp_toolkit.gp_size_of_table_disk
where 1=1
	and sotdschemaname = 'seminar_demo'
    and sotdtablename in ('customer_AO_column', 'customer_AO_row')
order by sotdsize desc, sotdtoastsize desc;

---

/* Способ, как можно посмотреть на занимаемое таблицей/партицией место */

SELECT
  sotdschemaname AS schemaname,
  sotdtablename AS tablename,
  SUM(sotdsize)/1024/1024/1024::NUMERIC(15,4) AS size_GB
FROM gp_toolkit.gp_size_of_table_disk
GROUP BY sotdtablename, sotdschemaname
ORDER BY 1;

---

/* Посмотрим на то, как работает партицирование;
   У нас у C_CUSTKEY 25 уникальных значений;
   Партицируем таблицу так, чтобы строки со значениями C_CUSTKEY от 1 до 5
   лежали в партициях согласно значению, а все остальные - в партиции с названием NATION;
   Должны получиться 5 маленьких и одна большая партиция;
*/

create table seminar_demo.customer_AO_column_partition (
	C_CUSTKEY INT, 
  C_NAME VARCHAR(25),
  C_ADDRESS VARCHAR(40),
  C_NATIONKEY INTEGER,
  C_PHONE CHAR(15),
  C_ACCTBAL DECIMAL(15,2),
  C_MKTSEGMENT CHAR(10),
  C_COMMENT VARCHAR(117)
)
WITH (APPENDONLY=True, COMPRESSLEVEL=7, ORIENTATION=COLUMN, COMPRESSTYPE=zlib)
DISTRIBUTED BY (C_CUSTKEY)
PARTITION BY RANGE (C_NATIONKEY)
(START (1) END (5) EVERY (1),
 DEFAULT PARTITION NATION);
 
insert into seminar_demo.customer_AO_column_partition
select * from public.customer;

SELECT
  sotdschemaname AS schemaname,
  sotdtablename AS tablename,
  SUM(sotdsize)/1024/1024/1024::NUMERIC(15,4) AS size_GB
FROM gp_toolkit.gp_size_of_table_disk
GROUP BY sotdtablename, sotdschemaname
ORDER BY 1;

---

/* Планы запросов - это интересно!
   Посмотрим на несколько планов запросов
   Если есть слово explain - покажутся план и кост, как его видит планировщик перед выполением запроса
*/

explain select * from seminar_demo.customer_AO_column_partition;
explain select * from seminar_demo.customer_AO_column_partition where C_NATIONKEY > 6;

/* Если добавить analyse - то запрос выполнится,
   а GP покажет фактические план и хост с худшей по перфу ноды
*/

explain analyse select * from seminar_demo.customer_AO_column_partition;
explain analyse select * from seminar_demo.customer_AO_column_partition where C_NATIONKEY > 6;

---

/* Посмотрим на то, как выглядит в плане JOIN */

explain
select *
from public.customer as c
inner join public.orders as o on 1=1
	and c.c_custkey = o.o_custkey
;

/* Нам не нравится, как ходит этот join, потому что данные передаются по сети между нодами.
   Попробуем пересегментировать таблицы по одному ключу, чтобы JOIN выполнялся локально внутри каждой ноды
   без необходимости передачи данных по сети.
*/

create table seminar_demo.customer_custkey as
select *
from public.customer
distributed by (c_custkey);

create table seminar_demo.orders_custkey as
select *
from public.orders
distributed by (o_custkey);

/* Посмотрите, как изменился план запроса.
   Стало лучше?
*/

explain
select *
from seminar_demo.customer_custkey as c
inner join seminar_demo.orders_custkey as o on 1=1
	and c.c_custkey = o.o_custkey
;

---

/* У GreenPlum 2 оптимизатора запросов - ORCA и Legacy.
   set optimizer=off; - используется Legacy.
   set optimizer=on;  - используется ORCA.
   Посмотрим, как отличаются планы запросов при использовании разных оптимизаторов
*/

set optimizer=off;
explain
select *
from seminar_demo.customer_custkey as c
inner join seminar_demo.orders_custkey as o on 1=1
	and c.c_custkey = o.o_custkey
;

set optimizer=on;
explain
select *
from seminar_demo.customer_custkey as c
inner join seminar_demo.orders_custkey as o on 1=1
	and c.c_custkey = o.o_custkey
;

---

set optimizer=off;
explain select * from seminar_demo.customer_AO_column_partition where C_NATIONKEY > 6;

set optimizer=on;
explain select * from seminar_demo.customer_AO_column_partition where C_NATIONKEY > 6;

---

set optimizer=off;
explain select * from seminar_demo.customer_AO_column_partition;

set optimizer=on;
explain select * from seminar_demo.customer_AO_column_partition;