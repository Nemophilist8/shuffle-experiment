#!/bin/bash

# 变量设置
JAR_PATH="target/scala-2.10/spark-shuffle-experiment-1.0.0.jar"
MASTER="spark://49.52.27.113:7077"
DATA_PATH="file:///tmp/tpch-dbgen/gen_data/lineitem-medium.tbl"
# CLASS_NAME="MainEntry"
MEMORY="1G"
TEST_DIR="logs/shuffle_file_test_$(date '+%Y%m%d_%H%M%S')"
mkdir -p $TEST_DIR

# 定义函数运行任务
run_test() {
    manager=$1
    mode=$2
    size=$3
    
    echo "------------------------------------------------"
    echo "Running Test: Manager=$manager, Mode=$mode", Size=$size
    echo "------------------------------------------------"

    local log_file="${TEST_DIR}/shuffle_${manager}_${mode}_${size}_tpch.log"
    
    echo "日志路径: $log_file"
    echo "执行参数: manager=${manager}, mode=${mode}, size=${size}"

    local event_log_dir="${TEST_DIR}/events_${manager}_${mode}"
    mkdir -p "$event_log_dir"
    
    spark-submit \
    --class edu.ecnu.MainEntry \
    --master $MASTER \
    --deploy-mode client \
    --executor-memory 1G \
    --driver-memory 512M \
    --executor-cores 2 \
    --conf spark.executor.instances=2 \
    --conf spark.dynamicAllocation.enabled=false \
    --total-executor-cores 4 \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.shuffle.manager=${manager} \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="$event_log_dir" \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.shuffle.consolidateFiles=false \
    --conf spark.shuffle.sort.bypassMergeThreshold=1 \
    $JAR_PATH "tpch" "$manager" "${DATA_PATH}" "$mode" "$size" 2>&1 | tee "$log_file"
}

# === 实验组 1: Hash Shuffle ===
run_test "hash" "uniform" "medium"
# run_test "hash" "longtail" "medium"
# run_test "hash" "skew" "medium"

# === 实验组 2: Sort Shuffle ===
run_test "sort" "uniform" "medium"
# run_test "sort" "longtail" "medium"
# run_test "sort" "skew" "medium"