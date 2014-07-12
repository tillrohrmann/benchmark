package org.stsffap.benchmarks

import eu.stratosphere.api.common.{Plan, PlanExecutor}

trait StratosphereBenchmark extends Benchmark{
  def executor: PlanExecutor
  def parallelism: Int

  def getPlan(config: RuntimeConfiguration, data: Map[String, String]): Plan

  def run(config: RuntimeConfiguration, data: Map[String, String]): Double = {
    val plan = getPlan(config, data)
    plan.setDefaultParallelism(parallelism)
    plan.setJobName("StratosphereBenchmark")

    executor.executePlan(plan).getNetRuntime/1000.0
  }

  def stop():Unit = {}

}
