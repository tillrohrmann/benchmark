package org.stsffap.benchmark

trait PageRankBenchmark {
  def getPageRankConfiguration(inputData: Map[String, String]): PageRankConfiguration = {
    PageRankConfiguration(numRows = inputData("rows").toInt, sparsity = inputData("sparsity")
      .toDouble, maxIterations = inputData("maxIterations").toInt)
  }
}
