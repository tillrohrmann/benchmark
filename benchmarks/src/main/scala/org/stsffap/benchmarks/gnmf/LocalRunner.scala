package org.stsffap.benchmarks.gnmf

import eu.stratosphere.client.LocalExecutor
import org.stsffap.benchmarks.RuntimeConfiguration

object LocalRunner {
  def main(args: Array[String]){
    val executor = new LocalExecutor()
    executor.setDefaultOverwriteFiles(true)
    val gnmf = new GNMFStratosphere(executor, 1)

    val runtimeConfig = RuntimeConfiguration(outputPath = "/tmp/benchmark/gnmf")
    val data = List("sparsity" -> ".9","rowsV" -> "10", "colsV" -> "5", "k" -> "2", "maxIterations" -> "2").toMap
    gnmf.run(runtimeConfig, data)
  }
}
