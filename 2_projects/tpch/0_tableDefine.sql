-- Databricks notebook source
USE CATALOG shared; 

USE SCHEMA tpch; 


-- COMMAND ----------

-- region
CREATE OR REPLACE TABLE region (
  `r_regionkey`  INT,
  `r_name`       CHAR(25),
  `r_comment`    VARCHAR(152),
  `r_dummy`      VARCHAR(10),
  PRIMARY KEY (`r_regionkey`)
  )
  TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
;

INSERT INTO region (
 SELECT 
  _c0 as  `r_regionkey`, 
  _c1 as  `r_name` ,
  _c2 as  `r_comment`,
  _c3 as  `n_dummy` 
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/region.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

select * from region; 


-- COMMAND ----------

-- nation
CREATE OR REPLACE TABLE nation (
  `n_nationkey`  INT,
  `n_name`       CHAR(25),
  `n_regionkey`  INT,
  `n_comment`    VARCHAR(152),
  `n_dummy`      VARCHAR(10),
   PRIMARY KEY (`n_nationkey`),
   FOREIGN KEY (`n_regionkey`) REFERENCES region (`r_regionkey`)
  )
  TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
  ;

INSERT INTO nation (
 SELECT 
  _c0 as  `n_nationkey`, 
  _c1 as  `n_name` ,
  _c2 as  `n_regionkey`,
  _c3 as  `n_comment`,
  _c4 as  `n_dummy` 
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/nation.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

-- COMMAND ----------

-- customer
CREATE OR REPLACE TABLE customer (
  `c_custkey`     INT,
  `c_name`        VARCHAR(25),
  `c_address`     VARCHAR(40),
  `c_nationkey`   INT,
  `c_phone`       CHAR(15),
  `c_acctbal`     DECIMAL(15,2),
  `c_mktsegment`  CHAR(10),
  `c_comment`     VARCHAR(117),
  `c_dummy`       VARCHAR(10),
  PRIMARY KEY (`c_custkey`),
  FOREIGN KEY (`c_nationkey`) REFERENCES nation (`n_nationkey`))
  TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
  ;

INSERT INTO customer (
 SELECT 
  _c0 as  `c_custkey`, 
  _c1 as  `c_name` ,
  _c2 as  `c_address`,
  _c3 as  `c_nationkey`,
  _c4 as  `c_phone`,
  _c5 as  `c_acctbal`,
  _c6 as  `c_mktsegment`,
  _c7 as  `c_comment`,
  _c8 as  `c_dummy`
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/customer.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

select * from customer; 

-- COMMAND ----------

-- part
CREATE OR REPLACE TABLE part (
  `p_partkey`     INT,
  `p_name`        VARCHAR(55),
  `p_mfgr`        CHAR(25),
  `p_brand`       CHAR(10),
  `p_type`        VARCHAR(25),
  `p_size`        INT,
  `p_container`   CHAR(10),
  `p_retailprice` DECIMAL(15,2) ,
  `p_comment`     VARCHAR(23) ,
  `p_dummy`       VARCHAR(10),
  PRIMARY KEY (`p_partkey`)
  )
  TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
  ;

INSERT INTO part (
 SELECT 
  _c0 as  `p_partkey`, 
  _c1 as  `p_name` ,
  _c2 as  `p_mfgr`,
  _c3 as  `p_brand`,
  _c4 as  `p_type`,
  _c5 as  `p_size`,
  _c6 as  `p_container`,
  _c7 as  `p_retailprice`,
  _c8 as  `p_comment`,
  _c9 as  `p_dummy`
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/part.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

select * from part; 


-- COMMAND ----------

-- partsupp
CREATE OR REPLACE TABLE partsupp (
  `ps_partkey`     INT,
  `ps_suppkey`     INT,
  `ps_availqty`    INT,
  `ps_supplycost`  DECIMAL(15,2),
  `ps_comment`     VARCHAR(199),
  `ps_dummy`       VARCHAR(10),
  PRIMARY KEY (`ps_suppkey`),
  FOREIGN KEY (`ps_partkey`) REFERENCES part (`p_partkey`))
   TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5') 
  ;

INSERT INTO partsupp (
 SELECT 
  _c0 as  `ps_partkey`, 
  _c1 as  `ps_suppkey` ,
  _c2 as  `ps_availqty`,
  _c3 as  `ps_supplycost`,
  _c4 as  `ps_comment`,
  _c5 as  `ps_dummy`
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/partsupp.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

select * from partsupp; 

-- COMMAND ----------

-- orders
CREATE OR REPLACE TABLE orders (
  `o_orderkey`       INT,
  `o_custkey`        INT,
  `o_orderstatus`    CHAR(1),
  `o_totalprice`     DECIMAL(15,2),
  `o_orderdate`      DATE,
  `o_orderpriority`  CHAR(15),
  `o_clerk`          CHAR(15),
  `o_shippriority`   INT,
  `o_comment`        VARCHAR(79),
  `o_dummy`          VARCHAR(10),
  PRIMARY KEY (`o_orderkey`),
  FOREIGN KEY (`o_custkey`) REFERENCES customer (`c_custkey`))
  TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
  
  ;
  

INSERT INTO orders (
 SELECT 
  _c0 as `o_orderkey`,
  _c1 as `o_custkey`,
  _c2 as `o_orderstatus`,
  _c3 as `o_totalprice`,
  _c4 as `o_orderdate`,
  _c5 as `o_orderpriority`,
  _c6 as `o_clerk`,
  _c7 as `o_shippriority`,
  _c8 as `o_comment`,
  _c9 as `o_dummy`
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/orders.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

select * from orders; 

-- COMMAND ----------


-- lineitem
CREATE OR REPLACE TABLE lineitem (
  `l_orderkey`    INT,
  `l_partkey`     INT,
  `l_suppkey`     INT,
  `l_linenumber`  INT,
  `l_quantity`    DECIMAL(15,2),
  `l_extendedprice`  DECIMAL(15,2),
  `l_discount`    DECIMAL(15,2),
  `l_tax`         DECIMAL(15,2),
  `l_returnflag`  CHAR(1),
  `l_linestatus`  CHAR(1),
  `l_shipdate`    DATE,
  `l_commitdate`  DATE,
  `l_receiptdate` DATE,
  `l_shipinstruct` CHAR(25),
  `l_shipmode`    CHAR(10),
  `l_comment`     VARCHAR(44),
  `l_dummy`       VARCHAR(10),
  FOREIGN KEY (`l_orderkey`) REFERENCES orders (`o_orderkey`),
  FOREIGN KEY (`l_partkey`) REFERENCES part (`p_partkey`),
  FOREIGN KEY (`l_suppkey`) REFERENCES partsupp (`ps_suppkey`)
  )
    TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
  
  ;

INSERT INTO lineitem (
 SELECT 
  _c0 as `l_orderkey`,
  _c1 as `l_partkey`,
  _c2 as `l_suppkey` ,
  _c3 as `l_linenumber`,
  _c4 as `l_quantity`,
  _c5 as `l_extendedprice`,
  _c6 as `l_discount`,
  _c7 as `l_tax`,
  _c8 as `l_returnflag`,
  _c9 as `l_linestatus`,
  _c10 as`l_shipdate`,
  _c11 as `l_commitdate`,
  _c12 as `l_receiptdate`,
  _c13 as `l_shipinstruct`,
  _c14 as `l_shipmode`,
  _c15 as `l_comment`,
  _c16 as `l_dummy`
  FROM read_files(
  '/Volumes/shared/tpch/raw_data/lineitem.csv.gz',
  format => 'csv',
  header => 'false',
  delimiter => '|'
)
)
;

select * from lineitem; 
