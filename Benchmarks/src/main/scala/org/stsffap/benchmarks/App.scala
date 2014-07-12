package org.stsffap.benchmarks

import eu.stratosphere.client.LocalExecutor
import org.apache.spark.SparkConf
import org.stsffap.benchmarks.pageRank.{PageRankStratosphere, PageRankSpark, PageRankConfiguration}

object App {

  def main(args: Array[String]){
    val dop = 4;

    val sparkConf = new SparkConf().
      setMaster("local["+dop+"]").
      setAppName("PageRankSpark").
      set("spark.cores.max", dop.toString).
      set("spark.default.parallelism", dop.toString).
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val pageRank = new PageRankSpark(sparkConf)

    val configuration = PageRankConfiguration(numRows = 10, sparsity = 0.1, maxIterations = 100)
    val runtimeConfiguration = RuntimeConfiguration(outputPath = "file:///tmp/benchmark/")
//    pageRank.executePageRank(configuration)

    val executor = new LocalExecutor
    executor.setDefaultOverwriteFiles(true)

    val pageRankStratosphere = new PageRankStratosphere(executor, dop)

    pageRankStratosphere.execute(runtimeConfiguration, configuration)


  }

}
