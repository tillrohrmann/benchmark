package org.stsffap.benchmarks

import org.apache.flink.api.scala.ExecutionEnvironment

trait FlinkBenchmark extends Benchmark{
  def env: ExecutionEnvironment
  def parallelism: Int

  def createProgram(config: RuntimeConfiguration, data: Map[String, String])

  def run(config: RuntimeConfiguration, data: Map[String, String]): Double = {
    createProgram(config, data)
    env.setParallelism(parallelism)

    val result = env.execute("FlinkBenchmark")

    result.getNetRuntime/1000.0
  }

  def stop():Unit = {}

}
