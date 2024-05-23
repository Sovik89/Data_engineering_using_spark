db_name=${1}
table_name=${2}
date=${3}

spark-sql --conf spark.sql.warehouse.dir=/user/`whoami`/spark/warehouse \
          --packages io.delta:delta-spark_2.12:3.0.0 \
          --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
          -f spark_sql_scripts/getting_started.sh \
          -d DB_NAME="${db_name}" \
          -d TABLE_NAME="${table_name}" \
          -d ORDER_DATE="${date}" 1>${table_name}_by_date_out.txt 2>${table_name}_by_date_log.log
          