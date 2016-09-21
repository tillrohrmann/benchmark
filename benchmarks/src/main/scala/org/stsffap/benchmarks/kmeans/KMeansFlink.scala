package org.stsffap.benchmarks.kmeans

import breeze.stats.distributions.Rand
import org.apache.flink.api.scala._
import org.stsffap.benchmarks.{FlinkBenchmark, RuntimeConfiguration}

@SerialVersionUID(1L)
class KMeansFlink(@transient val env: ExecutionEnvironment, val parallelism: Int) extends FlinkBenchmark
with KMeansBenchmark with Serializable{
  var kmeans: KMeansConfiguration = null

  def getScalaPlan(runtimeConfig: RuntimeConfiguration, kmeansConfig: KMeansConfiguration): Unit = {
    this.kmeans = kmeansConfig

    val datapoints = getDatapoints(kmeans)
    val initialCentroids = getCentroids(kmeans)

    val resultingCentroids = initialCentroids.iterate(kmeans.maxIterations){(centroids) => {
      val distances = datapoints cross centroids apply { (left, right) =>
        val ((dataId, datapoint), (centroidId, centroid)) = (left, right)
        (dataId, centroidId, datapoint, centroid.sqrDistance(datapoint))
      }

      val assignment = distances
        .groupBy(x => x._1)
        .reduce((a, b) => if (a._4 < b._4) a else b)
        .map{
          input =>
            val (_, centroidId, datapoint,_) = input
            (centroidId, datapoint, 1)}

      assignment.groupBy(x => x._1).reduce((a, b) => (a._1, a._2 + b._2, a._3 + b._3)).map{
        input =>
          val (id, data, num) = input
          (id, data/num)
      }
    }}

    val sink = resultingCentroids.writeAsCsv(runtimeConfig.outputPath+"/result")
  }

  def getDatapoints(kmeans: KMeansConfiguration) = {
    val ids = 0 until kmeans.numDatapoints

    env.fromCollection(ids) map { id =>
      (id, Datapoint(Rand.uniform.draw(), Rand.uniform.draw()))
    }
  }

  def getCentroids(kmeans: KMeansConfiguration) = {
    val ids = 0 until kmeans.numCentroids

    env.fromCollection(ids) map { id =>
      (id, Datapoint(Rand.uniform.draw(), Rand.uniform.draw()))
    }
  }


  def createProgram(runtimeConfig: RuntimeConfiguration, data: Map[String, String]) = {
    getScalaPlan(runtimeConfig, getKMeansConfiguration(data))
  }

  def getInformation: String = {
    "# KMeans Benchmark Stratosphere"
  }
}
