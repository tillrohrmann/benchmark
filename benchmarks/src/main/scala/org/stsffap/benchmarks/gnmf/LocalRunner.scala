package org.stsffap.benchmarks.gnmf

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.stsffap.benchmarks.RuntimeConfiguration

object LocalRunner {
  def main(args: Array[String]){
    val data = List("sparsity" -> ".9","rowsV" -> "10", "colsV" -> "5", "k" -> "2", "maxIterations" -> "2").toMap
    val runtimeConfig = RuntimeConfiguration(outputPath = "/tmp/benchmark/gnmf")

    val configuration = new Configuration()
    configuration.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, true)

    val env = ExecutionEnvironment.createLocalEnvironment(configuration)

    val benchmark = new GNMFFlink(env, 1)

//    val conf = new SparkConf().
//    setMaster("local[2]").
//    setAppName("Local test")
//
//    val benchmark = new GNMFSpark(conf)

    benchmark.run(runtimeConfig, data)
  }
}
