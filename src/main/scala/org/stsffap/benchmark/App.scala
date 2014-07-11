package org.stsffap.benchmark

import eu.stratosphere.client.LocalExecutor
import org.apache.spark.{SparkContext, SparkConf}

object App {

  def main(args: Array[String]){
    val dop = 4;

    val sparkConf = new SparkConf().
      setMaster("local["+dop+"]").
      setAppName("PageRankSpark").
      set("spark.cores.max", dop.toString).
      set("spark.default.parallelism", dop.toString).
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    val pageRank = new PageRankSpark(sc)

    val configuration = PageRankConfiguration(numRows = 10, sparsity = 0.1, maxIterations = 100,
      outputPath = "file:///tmp/benchmark/")

//    pageRank.executePageRank(configuration)

    sc.stop()

    val executor = new LocalExecutor
    executor.setDefaultOverwriteFiles(true)

    val pageRankStratosphere = new PageRankStratosphere(executor, dop)

    pageRankStratosphere.execute(configuration)
  }

}
