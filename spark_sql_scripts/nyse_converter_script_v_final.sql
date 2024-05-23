
-- setting configs
create database if not exists nyse_db;
use nyse_db;
--stage table
create table if not exists nyse_stg(
    ticker STRING,
    trade_date INT,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT
)USING CSV
OPTIONS(
    path="/user/sovik/data/nyse"
);

-- target table

create table if not exists nyse_daily(
    ticker STRING,
    trade_date INT,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT
)USING DELTA 
PARTITIONED BY (trademonth INT);

-- refresh if still with previous values
REFRESH TABLE nyse_stg;

-- Insert into the target table
insert into table nyse_daily PARTITION (trademonth)
select ns.*,substr(trade_date,1,6) as trademonth from nyse_stg as ns;

-- verify

select count(*) from nyse_stg;
select count(*) from nyse_daily;

select substr(trade_date,1,4) AS tradeyear, count(*) as trade_count from nyse_daily
Group by tradeyear
order by tradeyear;