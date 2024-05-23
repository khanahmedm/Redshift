# Redshift
Redshift lab exercises

1. [RedShift Table Design](#redshift-table-design)
2. [Load Tables](#load-tables)
3. [Ongoing Loads - ELT](#ongoing-loads)
    1. [Stored Procedure](#stored-procedure)
    2. [Stored Procedure - Handling Exception](#stored-procedure---exception-handling)
    3. [Materialized Views](#materialized-views)
    4. [Functions](#functions)
4. [Data Sharing](#data-sharing)
    1. [Create data share on producer](#create-data-share-on-producer)
    2. [Query data from consumer](#query-data-from-consumer)
    3. [Create external schema in consumer](#create-external-schema-in-consumer)
    4. [Load local data and join to shared data](#load-local-data-and-join-to-shared-data)
6. [Machine Learning](#machine-learning)
7. [Query Data Lake](#query-data-lake)

## Redshift Table Design

```sql
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

CREATE TABLE region (
  R_REGIONKEY bigint NOT NULL,
  R_NAME varchar(25),
  R_COMMENT varchar(152))
diststyle all;

CREATE TABLE nation (
  N_NATIONKEY bigint NOT NULL,
  N_NAME varchar(25),
  N_REGIONKEY bigint,
  N_COMMENT varchar(152))
diststyle all;

create table customer (
  C_CUSTKEY bigint NOT NULL,
  C_NAME varchar(25),
  C_ADDRESS varchar(40),
  C_NATIONKEY bigint,
  C_PHONE varchar(15),
  C_ACCTBAL decimal(18,4),
  C_MKTSEGMENT varchar(10),
  C_COMMENT varchar(117))
diststyle all;

create table orders (
  O_ORDERKEY bigint NOT NULL,
  O_CUSTKEY bigint,
  O_ORDERSTATUS varchar(1),
  O_TOTALPRICE decimal(18,4),
  O_ORDERDATE Date,
  O_ORDERPRIORITY varchar(15),
  O_CLERK varchar(15),
  O_SHIPPRIORITY Integer,
  O_COMMENT varchar(79))
distkey (O_ORDERKEY)
sortkey (O_ORDERDATE);

create table part (
  P_PARTKEY bigint NOT NULL,
  P_NAME varchar(55),
  P_MFGR  varchar(25),
  P_BRAND varchar(10),
  P_TYPE varchar(25),
  P_SIZE integer,
  P_CONTAINER varchar(10),
  P_RETAILPRICE decimal(18,4),
  P_COMMENT varchar(23))
diststyle all;

create table supplier (
  S_SUPPKEY bigint NOT NULL,
  S_NAME varchar(25),
  S_ADDRESS varchar(40),
  S_NATIONKEY bigint,
  S_PHONE varchar(15),
  S_ACCTBAL decimal(18,4),
  S_COMMENT varchar(101))
diststyle all;

create table lineitem (
  L_ORDERKEY bigint NOT NULL,
  L_PARTKEY bigint,
  L_SUPPKEY bigint,
  L_LINENUMBER integer NOT NULL,
  L_QUANTITY decimal(18,4),
  L_EXTENDEDPRICE decimal(18,4),
  L_DISCOUNT decimal(18,4),
  L_TAX decimal(18,4),
  L_RETURNFLAG varchar(1),
  L_LINESTATUS varchar(1),
  L_SHIPDATE date,
  L_COMMITDATE date,
  L_RECEIPTDATE date,
  L_SHIPINSTRUCT varchar(25),
  L_SHIPMODE varchar(10),
  L_COMMENT varchar(44))
distkey (L_ORDERKEY)
sortkey (L_RECEIPTDATE);

create table partsupp (
  PS_PARTKEY bigint NOT NULL,
  PS_SUPPKEY bigint NOT NULL,
  PS_AVAILQTY integer,
  PS_SUPPLYCOST decimal(18,4),
  PS_COMMENT varchar(199))
diststyle even;
```

[go to top](#redshift)

## Load Tables
```sql
COPY region FROM 's3://redshift-labs/data/region/region.tbl.lzo'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy customer from 's3://redshift-labs/data/customer/customer.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy orders from 's3://redshift-labs/data/orders/orders.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy part from 's3://redshift-labs/data/part/part.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy supplier from 's3://redshift-labs/data/supplier/supplier.json' manifest
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy lineitem from 's3://redshift-labs/data/lineitem-part/'
iam_role default
region 'us-west-2' gzip delimiter '|' COMPUPDATE PRESET;

copy partsupp from 's3://redshift-labs/data/partsupp/partsupp.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;
```

### Load Validations
```sql
-- Load Validation
 --Number of rows= 5
select count(*) from region;

 --Number of rows= 25
select count(*) from nation;

 --Number of rows= 76,000,000
select count(*) from orders;
```

### Troubleshooting Loads
```sql
COPY customer FROM 's3://redshift-labs/data/nation/nation.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' noload;

select * from SYS_LOAD_ERROR_DETAIL;
```

### ANALYZE and VACUUM commands
```sql
ANALYZE orders;

select "table", size, tbl_rows, estimated_visible_rows
from SVV_TABLE_INFO
where "table" = 'orders';

delete orders where o_orderdate between '1997-01-01' and '1998-01-01';

select "table", size, tbl_rows, estimated_visible_rows
from SVV_TABLE_INFO
where "table" = 'orders';

vacuum delete only orders;

vacuum sort only orders;

vacuum recluster orders;

vacuum recluster orders boost;
```

[go to top](#redshift)

## Ongoing Loads
### Staging Table
```sql
create table stage_lineitem (
  L_ORDERKEY bigint NOT NULL,
  L_PARTKEY bigint,
  L_SUPPKEY bigint,
  L_LINENUMBER integer NOT NULL,
  L_QUANTITY decimal(18,4),
  L_EXTENDEDPRICE decimal(18,4),
  L_DISCOUNT decimal(18,4),
  L_TAX decimal(18,4),
  L_RETURNFLAG varchar(1),
  L_LINESTATUS varchar(1),
  L_SHIPDATE date,
  L_COMMITDATE date,
  L_RECEIPTDATE date,
  L_SHIPINSTRUCT varchar(25),
  L_SHIPMODE varchar(10),
  L_COMMENT varchar(44));
```
### Stored Procedure
```sql
CREATE OR REPLACE PROCEDURE lineitem_incremental()
AS $$
BEGIN

truncate stage_lineitem;  

copy stage_lineitem from 's3://redshift-immersionday-labs/data/lineitem/lineitem.tbl.340.lzo'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy stage_lineitem from 's3://redshift-immersionday-labs/data/lineitem/lineitem.tbl.341.lzo'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy stage_lineitem from 's3://redshift-immersionday-labs/data/lineitem/lineitem.tbl.342.lzo'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

merge into lineitem
using stage_lineitem
on stage_lineitem.l_orderkey = lineitem.l_orderkey
and stage_lineitem.l_linenumber = lineitem.l_linenumber
remove duplicates
;

END;
$$ LANGUAGE plpgsql;
```
### Row counts before calling stored procedure
```sql
SELECT count(*) FROM "dev"."public"."lineitem"; --303008217
SELECT count(*) FROM "dev"."public"."stage_lineitem"; --3000000
```

### Calling store procedure
```sql
call lineitem_incremental();
```

### Row count after calling stored procedure
```sql
SELECT count(*) FROM "dev"."public"."lineitem"; --306008217
```

## Stored Procedure - Exception Handling
### Create staging table
```sql
CREATE TABLE stage_lineitem2 (LIKE stage_lineitem);
```

### Create log table
```sql
CREATE TABLE procedure_log
(log_timestamp timestamp, procedure_name varchar(100), error_message varchar(255));
```

### Atomic
```sql
CREATE OR REPLACE PROCEDURE pr_divide_by_zero() AS
$$
DECLARE
	v_int int;
BEGIN
	v_int := 1/0;
EXCEPTION
	WHEN OTHERS THEN
		INSERT INTO procedure_log VALUES (getdate(), 'pr_divide_by_zero', sqlerrm);
		RAISE INFO 'pr_divide_by_zero: %', sqlerrm;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE pr_insert_stage() AS
$$
BEGIN
	TRUNCATE stage_lineitem2;

	INSERT INTO stage_lineitem2
	SELECT * FROM stage_lineitem;

	call pr_divide_by_zero();
EXCEPTION
	WHEN OTHERS THEN
		RAISE EXCEPTION 'pr_insert_stage: %', sqlerrm;
END;
$$
LANGUAGE plpgsql;
```
#### Call stored procedure
```sql
CALL pr_insert_stage();
```

#### Validate staging table
```sql
SELECT COUNT(*) FROM stage_lineitem2; --0
```

#### Check log table
```sql
SELECT * FROM procedure_log ORDER BY log_timestamp DESC;
```

### Non-atomic
```sql
CREATE OR REPLACE PROCEDURE pr_divide_by_zero_v2() NONATOMIC AS
$$
DECLARE
	v_int int;
BEGIN
	v_int := 1/0;
EXCEPTION
	WHEN OTHERS THEN
		INSERT INTO procedure_log VALUES (getdate(), 'pr_divide_by_zero_v2', sqlerrm);
		RAISE INFO 'pr_divide_by_zero_v2: %', sqlerrm;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_insert_stage_v2() NONATOMIC AS
$$
BEGIN
	TRUNCATE stage_lineitem2;

	INSERT INTO stage_lineitem2
	SELECT * FROM stage_lineitem;

	call pr_divide_by_zero_v2();
EXCEPTION
	WHEN OTHERS THEN
		RAISE EXCEPTION 'pr_insert_stage_v2: %', sqlerrm;
END;
$$
LANGUAGE plpgsql;
```

#### Calling non-atomic stored procedure
```sql
CALL pr_insert_stage_v2();
```

#### Validate staging table
```sql
SELECT COUNT(*) FROM stage_lineitem2;
```

#### Check log table
```sql
SELECT * FROM procedure_log ORDER BY log_timestamp DESC
```

## Materialized Views
### Query without materialized view
```sql
select n_name, s_name, l_shipmode,
  SUM(L_QUANTITY) Total_Qty
from lineitem
join supplier on l_suppkey = s_suppkey
join nation on s_nationkey = n_nationkey
where datepart(year, L_SHIPDATE) > 1997
group by 1,2,3
order by 3 desc
limit 1000;
```
### Create materialized view
```sql
CREATE MATERIALIZED VIEW supplier_shipmode_agg
AUTO REFRESH YES AS
select l_suppkey, l_shipmode, datepart(year, L_SHIPDATE) l_shipyear,
  SUM(L_QUANTITY)	TOTAL_QTY,
  SUM(L_DISCOUNT) TOTAL_DISCOUNT,
  SUM(L_TAX) TOTAL_TAX,
  SUM(L_EXTENDEDPRICE) TOTAL_EXTENDEDPRICE  
from LINEITEM
group by 1,2,3;
```

### Query with materialized view
```sql
select n_name, s_name, l_shipmode,
  SUM(TOTAL_QTY) Total_Qty
from supplier_shipmode_agg
join supplier on l_suppkey = s_suppkey
join nation on s_nationkey = n_nationkey
where l_shipyear > 1997
group by 1,2,3
order by 3 desc
limit 1000;
```

## Functions
### Python Function
```python
create function f_py_greater (a float, b float)
  returns float
stable
as $$
  if a > b:
    return a
  return b
$$ language plpythonu;

select f_py_greater (l_extendedprice, l_discount) from lineitem limit 10
```

### SQL Function
```sql
create function f_sql_greater (float, float)
  returns float
stable
as $$
  select case when $1 > $2 then $1
    else $2
  end
$$ language sql;  

select f_sql_greater (l_extendedprice, l_discount) from lineitem limit 10
```

[go to top](#redshift)

## Data Sharing

### Identify namespaces
#### Find producer namespace
```sql
-- This should be run on the producer 
select current_namespace;
```

### Find consumer namespace
```sql
-- This should be run on consumer 
select current_namespace;
```

### Create data share on producer
#### Create data share
```sql
CREATE DATASHARE cust_share SET PUBLICACCESSIBLE TRUE
```

#### Add schema to data share
```sql
ALTER DATASHARE cust_share ADD SCHEMA public;
```

#### Add customer table to data share
```sql
ALTER DATASHARE cust_share ADD TABLE public.customer;
```

#### View shared objects
```sql
show datashares;
select * from SVV_DATASHARE_OBJECTS;
```

#### Granting access to consumer cluster
```sql
Grant USAGE ON DATASHARE cust_share to NAMESPACE '<consumer namespace>'
```

### Query data from consumer

#### View shared objects
```sql
show datashares;
select * from SVV_DATASHARE_OBJECTS;
```

#### Create local database
```sql
CREATE DATABASE cust_db FROM DATASHARE cust_share OF NAMESPACE '<<producer namespace>';
```

#### Select query to check count
```sql
select count(*) from cust_db.public.customer; -- count 15000000
```

### Create external schema in consumer

#### Create local schema
```sql
CREATE EXTERNAL SCHEMA cust_db_public
FROM REDSHIFT
DATABASE 'cust_db'
SCHEMA 'public';
```

#### Select query to check the count
```sql
select count(*) from cust_db_public.customer; -- count 15000000
```

### Load local data and join to shared data

#### Create orders table in provisioned cluster (consumer).
```sql
DROP TABLE IF EXISTS orders;
create table orders
(  O_ORDERKEY bigint NOT NULL,  
O_CUSTKEY bigint,  
O_ORDERSTATUS varchar(1),  
O_TOTALPRICE decimal(18,4),  
O_ORDERDATE Date,  
O_ORDERPRIORITY varchar(15),  
O_CLERK varchar(15),  
O_SHIPPRIORITY Integer,  O_COMMENT varchar(79))
distkey (O_ORDERKEY)
sortkey (O_ORDERDATE);
```

#### Load orders table from public data set
```sql
copy orders from 's3://redshift-labs/data/orders/orders.tbl.'
iam_role default region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;
```

#### Select count to verify the data load
```sql
select count(*) from orders;  -- count 76000000
```

#### Joining locally loaded order data to shared customer data.
```sql
SELECT c_mktsegment, o_orderpriority, sum(o_totalprice)
FROM cust_db.public.customer c
JOIN orders o on c_custkey = o_custkey
GROUP BY c_mktsegment, o_orderpriority;
```

#### Using the local schema instead of the datashare database
```sql
SELECT c_mktsegment, o_orderpriority, sum(o_totalprice)
FROM cust_db_public.customer c
JOIN orders o on c_custkey = o_custkey
GROUP BY c_mktsegment, o_orderpriority;
```

[go to top](#redshift)

## Machine Learning

### Download the dataset
https://archive.ics.uci.edu/dataset/222/bank+marketing

### Create table for training data
```sql
CREATE TABLE bank_details_training(
   age numeric,
   jobtype varchar,
   marital varchar,
   education varchar,
   "default" varchar,
   housing varchar,
   loan varchar,
   contact varchar,
   month varchar,
   day_of_week varchar,
   duration numeric,
   campaign numeric,
   pdays numeric,
   previous numeric,
   poutcome varchar,
   emp_var_rate numeric,
   cons_price_idx numeric,     
   cons_conf_idx numeric,     
   euribor3m numeric,
   nr_employed numeric,
   y boolean ) ;

COPY bank_details_training from 's3://redshift-downloads/redshift-ml/workshop/bank-marketing-data/training_data/' REGION 'us-east-1' IAM_ROLE default CSV IGNOREHEADER 1 delimiter ';';
```

### Create a table for inference data
```sql
CREATE TABLE bank_details_inference(
   age numeric,
   jobtype varchar,
   marital varchar,
   education varchar,
   "default" varchar,
   housing varchar,
   loan varchar,
   contact varchar,
   month varchar,
   day_of_week varchar,
   duration numeric,
   campaign numeric,
   pdays numeric,
   previous numeric,
   poutcome varchar,
   emp_var_rate numeric,
   cons_price_idx numeric,     
   cons_conf_idx numeric,     
   euribor3m numeric,
   nr_employed numeric,
   y boolean ) ;

COPY bank_details_inference from 's3://redshift-downloads/redshift-ml/workshop/bank-marketing-data/inference_data/' REGION 'us-east-1' IAM_ROLE default CSV IGNOREHEADER 1 delimiter ';';
```

### Create a S3 bucket


### Create model
```sql

DROP MODEL model_bank_marketing;


CREATE MODEL model_bank_marketing
FROM (
SELECT    
   age ,
   jobtype ,
   marital ,
   education ,
   "default" ,
   housing ,
   loan ,
   contact ,
   month ,
   day_of_week ,
   duration ,
   campaign ,
   pdays ,
   previous ,
   poutcome ,
   emp_var_rate ,
   cons_price_idx ,     
   cons_conf_idx ,     
   euribor3m ,
   nr_employed ,
   y
FROM
    bank_details_training )
    TARGET y
FUNCTION func_model_bank_marketing
IAM_ROLE default
SETTINGS (
  S3_BUCKET '<<S3 bucket>>',
  MAX_RUNTIME 3600
  )
;
```

### Check status of the model
```sql
show model model_bank_marketing;
```

### Check accuracy and run inference query
```sql
--Inference/Accuracy on inference data

WITH infer_data
 AS (
    SELECT  y as actual, func_model_bank_marketing(age,jobtype,marital,education,"default",housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp_var_rate,cons_price_idx,cons_conf_idx,euribor3m,nr_employed) AS predicted,
     CASE WHEN actual = predicted THEN 1::INT
         ELSE 0::INT END AS correct
    FROM bank_details_inference
    ),
 aggr_data AS (
     SELECT SUM(correct) as num_correct, COUNT(*) as total FROM infer_data
 )
 SELECT (num_correct::float/total::float) AS accuracy FROM aggr_data;

--Predict how many will subscribe for term deposit vs not subscribe

WITH term_data AS ( SELECT func_model_bank_marketing( age,jobtype,marital,education,"default",housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp_var_rate,cons_price_idx,cons_conf_idx,euribor3m,nr_employed) AS predicted
FROM bank_details_inference )
SELECT
CASE WHEN predicted = 'Y'  THEN 'Yes-will-do-a-term-deposit'
     WHEN predicted = 'N'  THEN 'No-term-deposit'
     ELSE 'Neither' END as deposit_prediction,
COUNT(1) AS count
from term_data GROUP BY 1;
```

### Check model explainability
```sql
select explain_model('model_bank_marketing');
```

[go to top](#redshift)

## Query Data Lake


[go to top](#redshift)


