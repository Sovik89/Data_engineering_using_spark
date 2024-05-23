NAMENODE=${1}
HDFS_SCRIPT_DIR=${2}
USERNAME=${3}

hive -f hdfs://${NAMENODE}/${HDFS_SCRIPT_DIR}/nyse_dbloader_script_v2.hql --hivevar user_name=${USERNAME} --verbose


