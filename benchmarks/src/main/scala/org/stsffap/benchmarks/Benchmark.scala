package org.stsffap.benchmarks

trait Benchmark {
  def run(config: RuntimeConfiguration, data: Map[String, String]): Double

  def stop():Unit

  def getInformation: String
}
