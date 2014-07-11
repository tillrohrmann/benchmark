package org.stsffap.benchmark

trait Benchmark {
  def run(data: Map[String, String]): Double

  def getInformation: String
}
