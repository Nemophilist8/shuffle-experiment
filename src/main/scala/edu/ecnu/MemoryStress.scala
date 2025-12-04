package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf, SparkEnv}
import org.apache.spark.scheduler._
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


object MemoryStressMonitor {
    def run(args: Array[String]): Unit = {
        println("Running Memory Stress Monitor with args: " + args.mkString(" "))
        // Implementation of memory stress monitoring


    }
}

class MemoryMetricsListener extends SparkListener {
  private var shuffleWriteBytes: Long = 0L
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    if (metrics != null) metrics.shuffleWriteMetrics.foreach(m => shuffleWriteBytes += m.shuffleBytesWritten)
  }
  def getShuffleWriteBytes: Long = shuffleWriteBytes
}