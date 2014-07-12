package org.stsffap.benchmarks.gnmf

trait GNMFBenchmark {
  def getGNMFConfiguration(data: Map[String, String]): GNMFConfiguration = {
    GNMFConfiguration(rowsV = data("rowsV").toInt,colsV = data("colsV").toInt, k = data("k").toInt,
      sparsity = data("sparsity").toDouble, maxIterations = data("maxIterations").toInt)
  }
}
