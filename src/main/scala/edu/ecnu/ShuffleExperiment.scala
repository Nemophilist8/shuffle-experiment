package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.scheduler._

class ShuffleMetricsListener extends SparkListener {
  private var shuffleWriteBytes: Long = 0L
  
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    if (metrics != null) {
      metrics.shuffleWriteMetrics.foreach { writeMetrics =>
        shuffleWriteBytes += writeMetrics.shuffleBytesWritten
      }
    }
  }
  
  def getShuffleWriteBytes: Long = shuffleWriteBytes
  def reset(): Unit = shuffleWriteBytes = 0L
}

object ShuffleExperiment {
  
  // 统一逻辑：两个实验都使用 groupByKey，确保数据量一致，公平对比 Shuffle 机制
  def runExperiment(df: DataFrame, sc: SparkContext, name: String): (Long, Long) = {
    val listener = new ShuffleMetricsListener
    sc.addSparkListener(listener)
    
    println(s"开始运行 $name 实验 (使用 groupByKey)...")
    val startTime = System.currentTimeMillis()
    
    // 强制把 DataFrame 转换成 RDD 并打散，确保产生 Shuffle
    // 使用 groupByKey 产生巨大的 Shuffle 压力（无 Map 端预聚合）
    val result = df.rdd
      .map(row => ((row.getString(2), row.getString(5)), row.getDouble(4))) 
      .groupByKey() // 关键修改：从 reduceByKey 改为 groupByKey
      .count()
    
    // 等待 Listener 异步更新
    Thread.sleep(3000)
    
    val endTime = System.currentTimeMillis()
    val shuffleBytes = listener.getShuffleWriteBytes

    // sc.removeSparkListener(listener)
    // 减去 sleep 的时间
    (endTime - startTime - 3000, shuffleBytes)
  }
  
  def formatBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    if (bytes <= 0) return "0 B"
    val digitGroups = (Math.log10(bytes.toDouble) / Math.log10(1024)).toInt
    val unit = units(Math.min(digitGroups, units.length - 1))
    val value = bytes / Math.pow(1024, Math.min(digitGroups, units.length - 1))
    f"$value%.2f $unit"
  }

  def main(args: Array[String]): Unit = {
    // 从命令行接收 "hash" 或 "sort"
    val mode = if (args.length > 0) args(0) else "sort"
    
    val conf = new SparkConf()
      .setAppName(s"Shuffle-Experiment-$mode")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注意：spark.shuffle.manager 会通过 spark-submit 的 --conf 传入，这里不需要设置

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("WARN")

    try {
      println(s"=== 当前 Shuffle Manager: ${sc.getConf.get("spark.shuffle.manager", "default")} ===")
      val datasets = DataGenerator.generateDatasets(sqlContext)
      
      for ((size, df) <- datasets) {
        println(s"\n=== 测试数据集: $size, 记录数: ${df.count()} ===")
        // 预热
        df.rdd.count() 
        
        val (time, bytes) = runExperiment(df, sc, mode)
        println(s"结果 [$mode] - 耗时: ${time}ms, Shuffle数据量: ${formatBytes(bytes)}")
      }
    } finally {
      sc.stop()
    }
  }
}