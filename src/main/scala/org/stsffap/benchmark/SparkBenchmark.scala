package org.stsffap.benchmark

import org.apache.log4j.{SimpleLayout, WriterAppender, Level, Logger}

trait SparkBenchmark extends Benchmark {
  def runSparkBenchmark(data: Map[String, String])

  def run(data: Map[String, String]): Double = {
    val pattern = """Job finished: [^,]*, took ([0-9\.]*) s""".r
    val sparkLogger = Logger.getLogger("org.apache.spark.SparkContext")
    sparkLogger.setLevel(Level.INFO)
    val sparkTimer = new SparkTimer(pattern)
    sparkLogger.addAppender(new WriterAppender(new SimpleLayout(), sparkTimer))

    runSparkBenchmark(data)

    sparkTimer.totalTime
  }
}
