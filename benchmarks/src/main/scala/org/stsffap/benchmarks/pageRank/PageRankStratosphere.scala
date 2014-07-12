package org.stsffap.benchmarks.pageRank

import breeze.stats.distributions.Rand
import eu.stratosphere.api.common.{Plan, PlanExecutor}
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import eu.stratosphere.api.scala.{CollectionDataSource, DataSet, ScalaPlan}
import org.stsffap.benchmarks.{StratosphereBenchmark, RuntimeConfiguration}

@SerialVersionUID(1L)
class PageRankStratosphere(@transient val executor: PlanExecutor,val parallelism: Int) extends StratosphereBenchmark
with PageRankBenchmark with Serializable {
  var configuration: PageRankConfiguration = null

  def getScalaPlan(runtimeConfiguration: RuntimeConfiguration, config: PageRankConfiguration): ScalaPlan = {
    configuration = config
    val initialPageRankVector = getPageRankVector(configuration)
    val adjacencyMatrix = getAdjacencyMatrix(configuration)

    val stepFunction = (ds: DataSet[(Int, Double)]) => {
      val spreadRank = ds join adjacencyMatrix where {x => x._1} isEqualTo {y => y._1} flatMap {
        case ((id, rank), (_, adjList)) =>
          val length = adjList.length
          val votes = for(target <- adjList) yield (target, 0.85*rank/length)

          votes.+:((id, 0.15/configuration.numRows)).toIterator
      }

      spreadRank.groupBy(x => x._1).combinableReduceGroup(ps => ps.reduce((a,b) => (a._1, a._2 + b._2)))
    }

    val resultingPageRankVector = initialPageRankVector.iterate(configuration.maxIterations, stepFunction)

    val sink = resultingPageRankVector.write(runtimeConfiguration.outputPath, CsvOutputFormat[(Int, Double)]())

    new ScalaPlan(Seq(sink))
  }

  def getPageRankVector(configuration: PageRankConfiguration) = {
    val ids = 0 until configuration.numRows
    val src = CollectionDataSource(ids)
    src.groupBy{ x => x}.reduceGroup(xs => (xs.next, 1.0/configuration.numRows))
  }

  def getAdjacencyMatrix(configuration: PageRankConfiguration) = {
    val ids = 0 until configuration.numRows
    val src = CollectionDataSource(ids)
    src.groupBy(x => x).reduceGroup{xs =>
      val id = xs.next

      val adjList = Rand.uniform.sample(configuration.numRows).zipWithIndex.filter{p => p._2 == id || p._1 <
        configuration.sparsity}.map{_._2}

      (id, adjList)
    }
  }

  def getPlan(runtimeConfiguration: RuntimeConfiguration, data: Map[String, String]): Plan = {
    getScalaPlan(runtimeConfiguration, getPageRankConfiguration(data))
  }

  def getInformation:String = {
    "# PageRank Benchmark Stratosphere"
  }
}
