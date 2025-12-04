package edu.ecnu

object MainEntry {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <task_type> [args...]")
      System.err.println("Available tasks: file, memory, skew")
      System.exit(1)
    }

    // 第一个参数作为任务类型
    val taskType = args(0)
    // 剩余参数传递给具体任务
    val taskArgs = args.drop(1)

    println(s"=== Dispatcher: Routing to task [$taskType] ===")

    taskType.toLowerCase match {
      case "file" =>
        FileCountMonitor.run(taskArgs)

      case "memory" =>
        MemoryStressMonitor.run(taskArgs)

      case "skew" =>
        SkewExperiment.run(taskArgs)

      case "tpch" =>
        TPCHTest.run(taskArgs)

      case _ =>
        System.err.println(s"Unknown task type: $taskType")
        System.exit(1)
    }
  }
}