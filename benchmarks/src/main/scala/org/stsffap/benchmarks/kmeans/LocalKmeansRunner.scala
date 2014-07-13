package org.stsffap.benchmarks.kmeans

import eu.stratosphere.client.LocalExecutor
import org.apache.spark.SparkConf
import org.stsffap.benchmarks.RuntimeConfiguration

object LocalKmeansRunner {
  def main(args: Array[String]){
    val data = List("numCentroids" -> "3", "numDatapoints" -> "20", "maxIterations" -> "2").toMap
    val runtimeConfig = RuntimeConfiguration(outputPath = "/tmp/benchmark/kmeans")

//    val executor = new LocalExecutor()
//    executor.setDefaultOverwriteFiles(true)
//    val benchmark = new KMeansStratosphere(executor, 1)

    val conf = new SparkConf().
    setMaster("local[1]").
    setAppName("Local test")

    val benchmark = new KMeansSpark(conf)

    benchmark.run(runtimeConfig, data)
  }
}
