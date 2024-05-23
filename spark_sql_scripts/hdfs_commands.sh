spark-sql --conf spark.sql.warehouse.dir=/user/`whoami`/spark/warehouse \
          --packages io.delta:delta-spark_2.12:3.0.0 \
          --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
          -f /home/`whoami`/data-engineering-using-spark/spark_sql_scripts/nyse_converter_script_v_final.sql \
          --verbose 2>/dev/null

hdfs dfs -rm -R -skipTrash /user/`whoami`/data/nyse/*


