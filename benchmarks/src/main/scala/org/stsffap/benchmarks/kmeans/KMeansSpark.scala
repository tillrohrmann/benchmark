package org.stsffap.benchmarks.kmeans

import breeze.stats.distributions.Rand
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.stsffap.benchmarks.{RuntimeConfiguration, SparkBenchmark}

class KMeansSpark(val sparkConfig: SparkConf) extends SparkBenchmark with KMeansBenchmark {

  def execute(runtimeConfig: RuntimeConfiguration, kmeans: KMeansConfiguration){
    val datapoints = getDatapoints(kmeans)
    var centroids = getCentroids(kmeans)

    for(i <- 0 until kmeans.maxIterations){
      val distances = datapoints cartesian centroids map { case ((idPoint, datapoint:Datapoint),
      (idCentroid, centroid:Datapoint)) =>
        (idPoint, CentroidDistance(idCentroid, datapoint, datapoint.sqrDistance(centroid)))
      }

      val assignedDatapoints = distances.reduceByKey((a,b) => if(a.distance < b.distance) a else b) map { case (_,a) =>
        (a.centroidId, (a.datapoint, 1))
      }

      centroids = assignedDatapoints.reduceByKey{ case ((pointA, numA),(pointB, numB)) =>
        (pointA+pointB, numA + numB)
      } map {
        case (id, (point, num)) => (id, point / num)
      }
    }

    centroids.foreach{ println }
  }

  def getDatapoints(kmeans: KMeansConfiguration) = {
    val ids = 0 until kmeans.numDatapoints

    sc.parallelize(ids).map{ id =>
      (id, Datapoint(Rand.uniform.draw(), Rand.uniform.draw()))
    }
  }

  def getCentroids(kmeans: KMeansConfiguration) = {
    val ids = 0 until kmeans.numCentroids

    sc.parallelize(ids).map { id =>
      (id,Datapoint(Rand.uniform.draw(), Rand.uniform.draw()))
    }
  }

  def runSparkBenchmark(runtimeConfig: RuntimeConfiguration, data: Map[String, String]) {
    execute(runtimeConfig, getKMeansConfiguration(data))
  }

  def getInformation = {
    "# KMeans Benchmark Spark"
  }
}
