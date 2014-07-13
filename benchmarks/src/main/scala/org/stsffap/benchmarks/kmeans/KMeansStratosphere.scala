package org.stsffap.benchmarks.kmeans

import breeze.stats.distributions.Rand
import eu.stratosphere.api.common.{Plan, PlanExecutor}
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import eu.stratosphere.api.scala.{CollectionDataSource, ScalaPlan}
import org.stsffap.benchmarks.{RuntimeConfiguration, StratosphereBenchmark}

@SerialVersionUID(1L)
class KMeansStratosphere(@transient val executor: PlanExecutor, val parallelism: Int) extends StratosphereBenchmark
with KMeansBenchmark with Serializable{
  var kmeans: KMeansConfiguration = null

  def getScalaPlan(runtimeConfig: RuntimeConfiguration, kmeansConfig: KMeansConfiguration): ScalaPlan = {
    this.kmeans = kmeansConfig

    val datapoints = getDatapoints(kmeans)
    val initialCentroids = getCentroids(kmeans)

    val resultingCentroids = initialCentroids.iterate(kmeans.maxIterations, (centroids) => {
      val distances = datapoints cross initialCentroids map { case ((dataId, datapoint), (centroidId, centroid)) =>
        (dataId, centroidId, datapoint, centroid.distance(datapoint))
      }

      val assignment = distances.groupBy(x => x._1).combinableReduceGroup( points => points.minBy(_._4)).map{ case
        (_, centroidId, datapoint,_) => (centroidId, datapoint, 1)}

      assignment.groupBy(x => x._1).combinableReduceGroup { ps =>
        ps.reduce{(a,b) => (a._1, a._2 + a._2, a._3 + a._3)}
      }.map{
        case (id, data, num) => (id, data/num)
      }
    })

    val sink = resultingCentroids.write(runtimeConfig.outputPath, CsvOutputFormat[(Int, Datapoint)]())

    new ScalaPlan(Seq(sink))
  }

  def getDatapoints(kmeans: KMeansConfiguration) = {
    val ids = 0 until kmeans.numDatapoints

    CollectionDataSource(ids) map { id =>
      (id, Datapoint(Rand.uniform.draw(), Rand.uniform.draw()))
    }
  }

  def getCentroids(kmeans: KMeansConfiguration) = {
    val ids = 0 until kmeans.numCentroids

    CollectionDataSource(ids) map { id =>
      (id, Datapoint(Rand.uniform.draw(), Rand.uniform.draw()))
    }
  }


  def getPlan(runtimeConfig: RuntimeConfiguration, data: Map[String, String]): Plan = {
    getScalaPlan(runtimeConfig, getKMeansConfiguration(data))
  }

  def getInformation: String = {
    "# KMeans Benchmark Stratosphere"
  }
}
