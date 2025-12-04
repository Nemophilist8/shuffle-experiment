package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf, SparkEnv}
import org.apache.spark.scheduler._
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID

object TPCHTest {

  def run(args: Array[String]): Unit = {
    // 参数变化:
    // args(0): Shuffle模式 (hash/sort)
    // args(1): TPC-H 数据文件路径 (file:///...)
    // args(2): 场景模式 (uniform/longtail/skew)
    
    val mode = if (args.length > 0) args(0) else "sort"
    val inputPath = if (args.length > 1) args(1) else "file:///tmp/tpch/lineitem.tbl"
    val scenario = if (args.length > 2) args(2) else "uniform"    
    // val conf = new SparkConf()
    //   .set("spark.shuffle.compress", "false")
    //   .set("spark.shuffle.spill.compress", "false")
    //   // 增加缓冲区，让 Sort Shuffle 更有优势
    //   .set("spark.eventLog.enabled", "true")
    //   .set("spark.eventLog.dir", "/tmp/spark-events")

    val conf = new SparkConf()
      .setAppName(s"Shuffle-Exp-$mode-$scenario")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 动态设置 Shuffle Manager
      .set("spark.shuffle.manager", mode) 
      // 确保 Hash Shuffle 不自动合并文件 (为了测试 file explosion)
      .set("spark.shuffle.consolidateFiles", "false")
      .set("spark.local.dir", "/tmp/spark/work")

    val sc = new SparkContext(conf)
    try {
      val sqlContext = new SQLContext(sc)
      sc.setLogLevel("WARN")

      println(s"=== Shuffle 实验配置 ===")
      println(s"Manager:  $mode")
      println(s"Input:    $inputPath")
      println(s"Scenario: $scenario")
      
      // 使用新的 TPC-H 加载器
      val df = DataGenerator.loadTpch(sqlContext, inputPath, scenario)
      
      println(">>> 正在预热 (Cache & Count)...")
      // Cache 确保数据已经加载到内存/磁盘，Shuffle 计时只包含 Shuffle 过程，不包含从 TextFile 读取解析的过程
      df.cache()
      val count = df.count()
      println(s">>> 预热完成，记录数: $count")
      
      // 运行实验
      // val (time, bytes, fileCount, writeBytes, diskSpill, gcTime, condition) = runExperiment(df, sc, s"$mode-$scenario")
      val resultCsv = runExperiment(df, sc, s"$mode-$scenario")
    

      println("\n" + "="*80)
      println("RESULT HEADER: Mode,Scenario,Count,Time(s),FileCount,WriteSize,DiskSpill,GCTime,Status")
      println(s"RESULT_CSV: $mode,$scenario,$resultCsv")
      println("="*80 + "\n")      
    } finally {
      sc.stop()
    }
  }
  
  def runExperiment(df: DataFrame, sc: SparkContext, name: String): String = {
    val listener = new tpchMetricsListener()
    sc.addSparkListener(listener)
    val monitor = new ShuffleFileMonitor()
    monitor.startMonitoring()
    
    println(s"\n=== 开始运行 $name 实验 ===")
    val startTime = System.currentTimeMillis()
    
    // 强制 2000 个 Reducer 以测试 LongTail
    val shufflePartitions = 2000 
    
    try {
      // DataGenerator 生成的列: 0:key, 1:value, 2:category, 3:payload
      // 1. 触发 Shuffle
      val count = df.rdd
        .map { row => (row.getString(0), row.getString(3)) }
        .groupByKey(shufflePartitions)
        .count() 
      
      val endTime = System.currentTimeMillis()
      Thread.sleep(5000) // 等待 Monitor 捕捉

      monitor.stopMonitoring()
      
      val duration = endTime - startTime
      // 获取详细指标
      val (writeBytes, diskSpill, memSpill, gcTime) = listener.getMetrics
      val fileCount = monitor.getMaxFileCount

      // === 修复点：使用 s"..." 格式化为字符串返回 ===
      // 格式: count, time, fileCount, writeBytes, diskSpill, gcTime, status
      // s"$count,${duration/1000.0},$fileCount,${formatBytes(writeBytes)},${formatBytes(diskSpill)},${gcTime}ms,Success"
      "$count,${duration/1000.0},$fileCount,$writeBytes,$diskSpill,$gcTime ms,Success"

    } catch {
      case e: Exception =>
        monitor.stopMonitoring()
        e.printStackTrace()
        // 捕获失败情况，保持 CSV 列数一致
        s"0,0,${monitor.getMaxFileCount},0,0,0,Failed-${e.getClass.getSimpleName}"
    }
  }
  
  def formatBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    if (bytes <= 0) return "0 B"
    val digitGroups = (Math.log10(bytes.toDouble) / Math.log10(1024)).toInt
    val unit = units(Math.min(digitGroups, units.length - 1))
    val value = bytes / Math.pow(1024, Math.min(digitGroups, units.length - 1))
    f"$value%.2f $unit"
  }
  
  def analyzeShuffleType(fileCount: Int, fileSizeKB: Long): String = {
    if (fileCount > 1000) s"Hash Shuffle (检测到大量小文件: $fileCount 个)"
    else if (fileCount <= 200) s"Sort Shuffle (文件高度合并: $fileCount 个)"
    else s"混合/不确定 ($fileCount 个文件)"
  }

  
}

class tpchShuffleFileMonitor(sparkLocalDir: String = "/tmp/spark/work") {
  private val executorService = Executors.newSingleThreadExecutor()
  private var isMonitoring = false
  private val maxFileCount = new AtomicInteger(0)
  private var maxTotalSize: Long = 0L
  
  def startMonitoring(intervalSeconds: Int = 1, durationSeconds: Int = 300): Unit = {
    isMonitoring = true
    val startTime = System.currentTimeMillis()
    
    executorService.submit(new Runnable {
      override def run(): Unit = {
        while (isMonitoring && 
               System.currentTimeMillis() - startTime < durationSeconds * 1000) {
          
          try {
            // 获取当前时间戳
            val timestamp = new java.text.SimpleDateFormat("HH:mm:ss")
              .format(new java.util.Date())
            
            // 查找shuffle文件
            val shuffleDir = new File(sparkLocalDir)
            val (fileCount, totalSizeKB) = if (shuffleDir.exists() && shuffleDir.isDirectory) {
              countShuffleFiles(shuffleDir)
            } else {
              (0, 0L)
            }
            
            // 更新最大值
            if (fileCount > maxFileCount.get()) {
              maxFileCount.set(fileCount)
            }
            if (totalSizeKB > maxTotalSize) {
              maxTotalSize = totalSizeKB
            }
            
            // 输出监控信息
            println(s"[ShuffleMonitor $timestamp] 文件数: $fileCount, " +
                   s"总大小: ${formatSizeKB(totalSizeKB)} (峰值: ${maxFileCount.get()}文件, ${formatSizeKB(maxTotalSize)})")
            
            // 如果发现文件数量异常多，可能是Hash Shuffle的特征
            if (fileCount > 1000) {
              println(s"[ShuffleMonitor 警告] 检测到大量临时文件($fileCount 个)，这可能是Hash Shuffle的特征")
            }
            
            // 等待指定间隔
            Thread.sleep(intervalSeconds * 1000)
          } catch {
            case e: Exception =>
              println(s"[ShuffleMonitor 错误] ${e.getMessage}")
          }
        }
      }
    })
  }
  
  private def countShuffleFiles(directory: File): (Int, Long) = {
    var fileCount = 0
    var totalSizeBytes: Long = 0L
    
    def scanDir(dir: File): Unit = {
      if (dir.exists() && dir.isDirectory) {
        val files = dir.listFiles()
        if (files != null) {
          files.foreach { file =>
            if (file.isFile && file.getName.startsWith("shuffle_")) {
              fileCount += 1
              totalSizeBytes += file.length()
            } else if (file.isDirectory) {
              scanDir(file)
            }
          }
        }
      }
    }
    
    scanDir(directory)
    val totalSizeKB = totalSizeBytes / 1024
    (fileCount, totalSizeKB)
  }
  
  private def formatSizeKB(kb: Long): String = {
    if (kb < 1024) s"${kb}KB"
    else if (kb < 1024 * 1024) f"${kb / 1024.0}%.2fMB"
    else f"${kb / (1024.0 * 1024.0)}%.2fGB"
  }
  
  def stopMonitoring(): Unit = {
    isMonitoring = false
    executorService.shutdown()
    
    println(s"\n[ShuffleMonitor 最终报告]")

    println(s"峰值文件数: ${maxFileCount.get()}")
    println(s"峰值文件大小: ${formatSizeKB(maxTotalSize)}")
    
    // 根据文件数量判断Shuffle类型
    val estimatedType = if (maxFileCount.get() > 500) "Hash Shuffle（文件数>500）" else "Sort Shuffle"
    println(s"推测Shuffle类型: $estimatedType")
  }
  
  def getMaxFileCount: Int = maxFileCount.get()
  def getMaxTotalSizeKB: Long = maxTotalSize
}

class tpchMetricsListener extends SparkListener {
  private var shuffleWriteBytes: Long = 0L
  private var diskBytesSpilled: Long = 0L
  private var memoryBytesSpilled: Long = 0L
  private var jvmGCTime: Long = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
val metrics = taskEnd.taskMetrics
    if (metrics != null) {
      metrics.shuffleWriteMetrics.foreach(m => shuffleWriteBytes += m.shuffleBytesWritten)
      diskBytesSpilled += metrics.diskBytesSpilled
      memoryBytesSpilled += metrics.memoryBytesSpilled
      jvmGCTime += metrics.jvmGCTime
    }
  }
  
  // def getShuffleWriteBytes: Long = shuffleWriteBytes
  // def reset(): Unit = shuffleWriteBytes = 0L
  def getMetrics: (Long, Long, Long, Long) = {
    (shuffleWriteBytes, diskBytesSpilled, memoryBytesSpilled, jvmGCTime)
  }
}