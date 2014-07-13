package org.stsffap.benchmarks.kmeans

case class Datapoint(x: Double, y: Double) {

  def distance(datapoint: Datapoint): Double = {
    math.sqrt(sqrDistance(datapoint))
  }

  def sqrDistance(datapoint: Datapoint): Double = {
    (x-datapoint.x)*(x-datapoint.x) + (y-datapoint.y)*(y-datapoint.y)
  }

  def +(datapoint: Datapoint) = {
    Datapoint(x+ datapoint.x, y + datapoint.y)
  }

  def /(dividend: Double) = {
    Datapoint(x/dividend, y/dividend)
  }
}
