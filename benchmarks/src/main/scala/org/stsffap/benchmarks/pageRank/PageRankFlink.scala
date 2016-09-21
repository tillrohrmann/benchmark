package org.stsffap.benchmarks.pageRank

import breeze.stats.distributions.Rand
import org.apache.flink.api.scala._
import org.stsffap.benchmarks.{FlinkBenchmark, RuntimeConfiguration}

@SerialVersionUID(1L)
class PageRankFlink(@transient val env: ExecutionEnvironment, val parallelism: Int) extends FlinkBenchmark
with PageRankBenchmark with Serializable {
  var configuration: PageRankConfiguration = null

  def getScalaPlan(runtimeConfiguration: RuntimeConfiguration, config: PageRankConfiguration): Unit = {
    configuration = config
    val initialPageRankVector = getPageRankVector(configuration)
    val adjacencyMatrix = getAdjacencyMatrix(configuration)

    val stepFunction = (ds: DataSet[(Int, Double)]) => {
      val spreadRank = ds join adjacencyMatrix where {x => x._1} equalTo {y => y._1} flatMap {
        input =>
          val ((id, rank), (_, adjList)) = input
          val length = adjList.length
          val votes = for(target <- adjList) yield (target, 0.85*rank/length)

          votes.+:((id, 0.15/configuration.numRows)).toIterator
      }

      spreadRank.groupBy(x => x._1).reduce((a, b) => (a._1, a._2 + b._2))
    }

    val resultingPageRankVector = initialPageRankVector.iterate(configuration.maxIterations)(stepFunction)

    resultingPageRankVector.writeAsCsv(runtimeConfiguration.outputPath)
  }

  def getPageRankVector(configuration: PageRankConfiguration) = {
    val ids = 0 until configuration.numRows
    val src = env.fromCollection(ids)
    src.groupBy{ x => x}.reduceGroup(xs => (xs.next, 1.0/configuration.numRows))
  }

  def getAdjacencyMatrix(configuration: PageRankConfiguration) = {
    val ids = 0 until configuration.numRows
    val src = env.fromCollection(ids)
    src.groupBy(x => x).reduceGroup{xs =>
      val id = xs.next

      val adjList = Rand.uniform.sample(configuration.numRows).zipWithIndex.filter{p => p._2 == id || p._1 <
        configuration.sparsity}.map{_._2}

      (id, adjList)
    }
  }

  def createProgram(runtimeConfiguration: RuntimeConfiguration, data: Map[String, String]): Unit = {
    getScalaPlan(runtimeConfiguration, getPageRankConfiguration(data))
  }

  def getInformation:String = {
    "# PageRank Benchmark Stratosphere"
  }
}
