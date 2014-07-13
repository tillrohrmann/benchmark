package org.stsffap.benchmarks.kmeans

trait KMeansBenchmark {
  def getKMeansConfiguration(data: Map[String, String]): KMeansConfiguration = {
    KMeansConfiguration(numDatapoints = data("numDatapoints").toInt,
    numCentroids = data("numCentroids").toInt,
    maxIterations = data("maxIterations").toInt
    )
  }
}
