package org.stsffap.benchmark

import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


class PageRankSpark(sc: SparkContext) extends SparkBenchmark with PageRankBenchmark {

  def executePageRank(configuration: PageRankConfiguration){
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
    }

    resultingPageRankVector foreach println
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


  def runSparkBenchmark(inputData: Map[String, String]) {
    executePageRank(getPageRankConfiguration(inputData))
    sc.stop()
  }
}
