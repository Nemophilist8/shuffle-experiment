package edu.ecnu

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._

/**
  * BasicExperiment.scala
  * 测试指标：吞吐量、Shuffle 写出量、运行时间
  */
object BasicExperiment {

  def runExperiment(sql: SQLContext, df: DataFrame, shufflePartitions: Int = 500)
      : (Long, Long, Double) = {

    val sc = sql.sparkContext

    // — Listener：统计 Shuffle 写出 —
    val listener = new ShuffleMetricsListener
    sc.addSparkListener(listener)

    val start = System.currentTimeMillis()

    val processedRecords = df.rdd
      .map(row => (row.getString(0), row.getString(3)))
      .groupByKey(shufflePartitions)
      .count()

    val end = System.currentTimeMillis()
    val durationMs = end - start

    val shuffleBytes = listener.shuffleBytes
    val throughput = processedRecords.toDouble / (durationMs / 1000.0)

    (durationMs, shuffleBytes, throughput)
  }

  def run(args: Array[String]): Unit = {
    println("Running Basic Experiment with args: " + args.mkString(" "))

    val mode = if (args.length > 0) args(0) else "sort"
    val datasetSize = if (args.length > 1) args(1) else "small"
    val shufflePartitions = if (args.length > 2) args(2).toInt else 500

    val conf = new SparkConf()
      .setAppName(s"BasicExperiment-$mode")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.shuffle.compress", "false")
      .set("spark.shuffle.spill.compress", "false")
      .set("spark.shuffle.file.buffer", "64k")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.manager", mode)
      .set("spark.local.dir", "/tmp/spark/work")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events")

    val sc = new SparkContext(conf)

    try {
      val sql = new SQLContext(sc)
      sc.setLogLevel("WARN")

      println(s"=== 基础 Shuffle 性能实验 ===")
      println(s"Spark 版本: ${sc.version}")
      println(s"Shuffle Manager 请求: $mode")
      println(s"Shuffle Manager 实际: ${sc.getConf.get("spark.shuffle.manager", "sort")}")
      println(s"数据集规模: $datasetSize")

      // 生成数据
      val df = datasetSize match {
        case "small-x" => DataGenerator.generateUniform(sql, "small-x")
        case "small"   => DataGenerator.generateUniform(sql, "small")
        case "medium"  => DataGenerator.generateUniform(sql, "medium")
        case "large"   => DataGenerator.generateUniform(sql, "large")
      }

      df.cache()
      df.count() // 预热

      println(">>> 预热完成，开始正式测试")

      val (duration, shuffleBytes, throughput) =
        runExperiment(sql, df, shufflePartitions)

      println("\n===== 测试结果 =====")
      println(s"运行时间: ${duration} ms")
      println(s"Shuffle 数据量: ${shuffleBytes} bytes")
      println(f"吞吐量: $throughput%.2f records/sec")

    } finally {
      sc.stop()
    }
  }
}

/**
  * Shuffle 指标 Listener
  */
class ShuffleMetricsListener extends SparkListener {
  @volatile var shuffleBytes: Long = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    if (metrics != null && metrics.shuffleWriteMetrics.isDefined) {
      shuffleBytes += metrics.shuffleWriteMetrics.get.shuffleBytesWritten
    }
  }
}
