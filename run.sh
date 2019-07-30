#!/usr/bin/env bash
source ~/.bash_profile
source ~/.bashrc

work_dir=$(cd $(dirname $0); pwd)
today=`date -d "-1 day" +%Y-%m-%d`

read_event_days='30'
read_event_path='hdfs://10.26.29.210:8020/user/hive/warehouse/event_info.db/b_read_chapter/ds='
read_event_gid_uid='hdfs://10.26.26.145:8020/rs/dingjing/knn/${today}/knn_'${read_event_days}'_gid_uid/'

spark_run="spark-submit --total-executor-cores=30 --executor-memory=20g "

### 函数
function hdfs_exist() {
    if [[ $# -ne 1 ]]
    then
        echo -e "请输入 hadoop 路径\n"
        return 1
    fi
    num=`hadoop fs -ls $1 | grep SUCCESS | wc -l`
    if [[ $num -ge 1 ]]
    then
        echo -e "${1}\t------>存在\n"
        return 0
    fi
    echo -e "${1}\t ------>不存在\n"
    return 1
}

# 准备数据
for((i=0;i<10;++i))
do
    hdfs_exist "${read_event_gid_uid}"
    if [[ $? -ne 0 ]]
    then
        cd ${work_dir}
        hadoop fs -rmr "${read_event_gid_uid}"
        ${spark_run} --class DataDetail ./jar/*.jar "${read_event_path}" "${today}" "${read_event_days}" "${read_event_gid_uid}"
        continue
    fi
    break
done



