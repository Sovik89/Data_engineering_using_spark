create database if not exists retail_db;
use retail_db;

create table if not exists orders(
    order_id INT,
    order_date STRING,
    order_customer_id INT,
    order_status STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;

# !ls -lrt "path";
# DESCRIBE formatted <table_name>;

LOAD DATA LOCAL INPATH '/home/sovik/data-engineering-using-spark/data/retail_db/orders/' into table orders;

select * from orders LIMIT 20;

# select count(*) as count_of_records from orders;

# show tables;
# select current_database();

# DROP table orders;