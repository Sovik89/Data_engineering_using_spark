db_name=${1}
table_name=${2}

spark-sql --conf spark.sql.warehouse.dir=/user/`whoami`/spark/warehouse \
          --packages io.delta:delta-spark_2.12:3.0.0 \
          --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
          -e "select count(*) from ${db_name}.${table_name}" 2>/dev/null 1>table_${table_name}_count.txt