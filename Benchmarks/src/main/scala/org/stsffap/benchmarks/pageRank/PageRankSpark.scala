package org.stsffap.benchmarks.pageRank

import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.stsffap.benchmarks.{SparkBenchmark, RuntimeConfiguration}


class PageRankSpark(val sparkConfig: SparkConf) extends SparkBenchmark with PageRankBenchmark {

  def executePageRank(runtimeConfiguration: RuntimeConfiguration, configuration: PageRankConfiguration){
    if(sc == null){
      init(runtimeConfiguration)
    }

    val initialPageRankVector = generatePageRankVector(configuration)
    val adjacencyMatrix = generateAdjacencyMatrix(configuration)

    var resultingPageRankVector: RDD[(Int, Double)] = initialPageRankVector

    for(i <- 0 until configuration.maxIterations){
      val joinedVectorAdjMatrix = resultingPageRankVector.join(adjacencyMatrix)
      val votes = joinedVectorAdjMatrix flatMap { case (id, (rank,adjList)) =>
        val length = adjList.length
        val spreadedRank = for(target <- adjList) yield {
          (target, 0.85*rank/length)
        }

        spreadedRank.+:((id, 0.15/configuration.numRows))
      }

      resultingPageRankVector = votes.reduceByKey(_ + _)

      if(runtimeConfiguration.iterationsUntilCheckpoint > 0 && i % runtimeConfiguration.iterationsUntilCheckpoint ==
        (runtimeConfiguration.iterationsUntilCheckpoint-1)){
        resultingPageRankVector.checkpoint()
      }
    }

    resultingPageRankVector foreach {
      p =>
        println(p)
    }
  }

  def generatePageRankVector(configuration: PageRankConfiguration) = {
    val numRows = configuration.numRows
    val vectorEntries = 0 until numRows map { id => (id, 1.0/numRows)}
    sc.parallelize(vectorEntries)
  }

  def generateAdjacencyMatrix(configuration: PageRankConfiguration) = {
    val numRows = configuration.numRows
    val entries = 0 until numRows
    val rows = sc.parallelize(entries)

    rows map { id =>
      val adjacencyList = Rand.uniform.sample(numRows).zipWithIndex.filter{p => p._2 == id || p._1 < configuration
        .sparsity}.map{_._2}
      (id, adjacencyList)
    }
  }


  def runSparkBenchmark(runtimeConfig: RuntimeConfiguration, inputData: Map[String, String]) {
    executePageRank(runtimeConfig: RuntimeConfiguration, getPageRankConfiguration(inputData))
  }

  def getInformation:String = {
    "# PageRank Benchmark: Spark"
  }
}
