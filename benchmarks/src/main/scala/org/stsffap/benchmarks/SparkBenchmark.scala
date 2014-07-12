package org.stsffap.benchmarks

import org.apache.log4j.{Level, Logger, SimpleLayout, WriterAppender}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkBenchmark extends Benchmark {
  def sparkConfig: SparkConf
  var sc: SparkContext = null;
  def runSparkBenchmark(runtimeConfig: RuntimeConfiguration, data: Map[String, String])

  def run(runtimeConfig: RuntimeConfiguration, data: Map[String, String]): Double = {
    val pattern = """Job finished: [^,]*, took ([0-9\.]*) s""".r
    val sparkLogger = Logger.getLogger("org.apache.spark.SparkContext")
    sparkLogger.setLevel(Level.INFO)
    val sparkTimer = new SparkTimer(pattern)
    sparkLogger.addAppender(new WriterAppender(new SimpleLayout(), sparkTimer))

    init(runtimeConfig)

    runSparkBenchmark(runtimeConfig, data)

    stop()

    sparkTimer.totalTime
  }

  def stop(){
    if(sc != null){
      sc.stop()
      sc = null
    }
  }

  def init(runtimeConfig: RuntimeConfiguration){
    sc = new SparkContext(sparkConfig)

    runtimeConfig.checkpointDir match {
      case Some(s) => sc.setCheckpointDir(s)
      case _ =>
    }
  }
}
