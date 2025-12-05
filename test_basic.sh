#!/bin/bash
# test_basic.sh - 测试 BasicExperiment 在不同 Shuffle Manager 与不同数据集规模下的表现

set -e

JAR_PATH="target/scala-2.10/spark-shuffle-experiment-2.0.0.jar"
MASTER="spark://49.52.27.113:7077"

# 测试输出目录
TEST_DIR="logs/shuffle_basic_test_$(date '+%Y%m%d_%H%M%S')"
mkdir -p "$TEST_DIR"

# 要测试的 Shuffle Manager 模式
MODES=("sort" "hash")

# 要测试的数据集规模
SIZES=("small" "medium" "large")

echo "===== BasicExperiment 自动测试开始 ====="
echo "输出目录：$TEST_DIR"
echo

for mode in "${MODES[@]}"; do
  for size in "${SIZES[@]}"; do
    
    # 日志文件名包含 mode 和 size
    log_file="${TEST_DIR}/basic_${mode}_${size}.log"

    echo
    echo ">>> 开始测试：mode=${mode}, size=${size}"
    echo "日志文件：$log_file"
    mkdir -p "$event_dir"

    # 提交 Spark 任务
    spark-submit \
        --class edu.ecnu.MainEntry \
        --master "$MASTER" \
        --deploy-mode client \
        --executor-memory 1G \
        --driver-memory 512M \
        --executor-cores 2 \
        --conf spark.executor.instances=2 \
        --conf spark.dynamicAllocation.enabled=false \
        --total-executor-cores 4 \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.shuffle.manager="${mode}" \
        --conf spark.eventLog.enabled=true \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.shuffle.consolidateFiles=false \
        --conf spark.shuffle.sort.bypassMergeThreshold=1 \
        "$JAR_PATH" "basic" "${mode}" "${size}" \
        2>&1 | tee "$log_file"

    echo ">>> 测试结束：mode=${mode}, size=${size}"
    echo

  done
done

echo "===== BasicExperiment 所有测试完成 ====="
echo "输出已保存到：$TEST_DIR"
