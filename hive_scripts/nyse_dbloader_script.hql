create database if not exists nyse_db;

use nyse_db;

create table if not exists nyse_daily(
    ticker STRING,
    trade_date INT,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT
) PARTITIONED BY (trademonth INT)
STORED AS PARQUET;

create table if not exists nyse_stg(
    ticker STRING,
    trade_date INT,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;

LOAD DATA LOCAL INPATH
'/home/sovik/data-engineering-using-spark/data/nyse_all/nyse_data/NYSE_${tradeyear}.txt.gz'
OVERWRITE INTO TABLE nyse_stg;

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE nyse_daily PARTITION (trademonth)
SELECT ns.*,SUBSTR(trade_date,1,6) AS trademonth
FROM nyse_stg as ns;

SHOW PARTITIONS nyse_daily;

select trademonth,count(*) as trade_count from nyse_daily
group by trademonth
order by trademonth;

