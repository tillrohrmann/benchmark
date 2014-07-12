package org.stsffap.benchmarks.gnmf

import eu.stratosphere.client.LocalExecutor
import org.apache.spark.SparkConf
import org.stsffap.benchmarks.RuntimeConfiguration

object LocalRunner {
  def main(args: Array[String]){
    val data = List("sparsity" -> ".9","rowsV" -> "10", "colsV" -> "5", "k" -> "2", "maxIterations" -> "2").toMap
    val runtimeConfig = RuntimeConfiguration(outputPath = "/tmp/benchmark/gnmf")

    val executor = new LocalExecutor()
    executor.setDefaultOverwriteFiles(true)
    val benchmark = new GNMFStratosphere(executor, 1)

//    val conf = new SparkConf().
//    setMaster("local[2]").
//    setAppName("Local test")
//
//    val benchmark = new GNMFSpark(conf)

    benchmark.run(runtimeConfig, data)
  }
}
