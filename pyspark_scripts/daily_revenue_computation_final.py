import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round
import logging

# initialize the environmnet variables
#SRC_BASE_DIR=/user/sovik/retail_db

src_base_dir=os.environ.get('SRC_BASE_DIR')
tgt_base_dir=os.environ.get('TGT_BASE_DIR')

# Initialize Spark Session
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=logging.INFO)
logging.info('Initialize Spark Session')
spark = SparkSession. \
    builder. \
    appName('Compute Daily Revenue'). \
    master('yarn'). \
    getOrCreate()

# Read orders data frame

logging.info('Read orders data frame')
orders = spark. \
    read. \
    csv(f"{src_base_dir}/orders",
        schema='''
        order_id INT, order_date STRING, order_customer_id INT, order_status STRING
        ''',
        header=False
    )

# Read order-items data frame
logging.info('Read order-items data frame')
order_items = spark. \
    read. \
    csv(f"{src_base_dir}/order_items",
        schema='''
        order_item_id INT,
        order_item_order_id INT,
        order_item_product_id INT,
        order_item_quantity INT,
        order_item_subtotal FLOAT,
        order_item_product_price FLOAT
        ''',
        header=False
    )

# Filter, join, group and aggregate to get daily revenue
logging.info('Filter, join, group and aggregate to get daily revenue generation')
orders_daily = orders.filter("order_status IN ('COMPLETE', 'CLOSED')"). \
    join(order_items, orders['order_id'] == order_items['order_item_order_id']). \
    groupBy('order_date'). \
    agg(round(sum('order_item_subtotal'), 2).alias('revenue')). \
    orderBy('order_date')

# Write the result to CSV
# orders_daily. \
#     write. \
#     mode('overwrite'). \
#     csv(f'{tgt_base_dir}/daily_revenue')

logging.info('Writing the result to CSV')
orders_daily. \
    write. \
    mode('overwrite'). \
    format('delta'). \
    save(f'{tgt_base_dir}/daily_revenue',header=True)

# Validate - Display and count rows in the resultant data frame
logging.info('Validate - Display and count rows in the resultant data frame')
orders_daily.show(30)
print("Total Rows:", orders_daily.count())