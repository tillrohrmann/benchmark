package org.stsffap.benchmarks.gnmf

import breeze.linalg._
import breeze.stats.distributions.{Rand, Gaussian}
import org.apache.spark.SparkConf
import org.stsffap.benchmarks.{RuntimeConfiguration, SparkBenchmark}
import org.apache.spark.SparkContext._

class GNMFSpark(val sparkConfig: SparkConf) extends SparkBenchmark with GNMFBenchmark{
  def execute(runtimeConfig: RuntimeConfiguration, gnmfConfiguration: GNMFConfiguration) {
    val V = getV(gnmfConfiguration)
    var W = getW(gnmfConfiguration)
    var H = getH(gnmfConfiguration)

    for(i <- 0 until gnmfConfiguration.maxIterations){
      //X = W'*V
      val rowV = V map { x => (x.row, x) }
      val partialX = rowV join W map {case (_, (entry, vector)) =>
        (entry.col, (vector * entry.value):DenseVector[Double])}
      val X = partialX.reduceByKey( _ + _ )

      //Y = W'*W*H
      val partialWW = W map { case (idx, row) =>
        val r = row.length
        DenseMatrix.ones[Double](r,r)
      }
      val WW = partialWW.reduce(_ + _)

      val Y = H map { case (idx, column) => (idx, WW*column)}
      // H <- H .* X ./ Y
      val XY = X join Y map { case (idx, (x,y)) => (idx, (x :/ y): DenseVector[Double])}
      H = H join XY map { case (idx, (h, xy)) => (idx, (h :* xy): DenseVector[Double])}


      // T = V*H'
      val colV = V map { x => (x.col, x) }
      val partialT = colV join H map { case (_, (entry, vector)) =>
        (entry.row, (vector * entry.value): DenseVector[Double])
      }
      val T = partialT.reduceByKey(_ + _)

      // U = W*H*H'
      val partialHH = H map { case (_, h) => (h * h.t):DenseMatrix[Double]}
      val HH = partialHH.reduce(_ + _)

      val U = W map { case (idx, row) => (idx, (HH * row): DenseVector[Double])}

      // W <- W .* T ./ U
      val TU = T join U map { case (idx, (t, u)) => (idx, (t :/ u): DenseVector[Double]) }
      W = W join TU map { case (idx, (w, tu)) => (idx, (w :* tu): DenseVector[Double])}
    }

    W map { case (idx, row) => (idx,row.activeSize)} foreach println
    H map { case (idx, col) => (idx,col.activeSize)} foreach println
  }

  def getV(gnmf: GNMFConfiguration) = {
    val coords = for(row <- 0 until gnmf.rowsV; col <- 0 until gnmf.colsV) yield (row, col)
    val gaussian = new Gaussian(0, 1)
    val entries = coords zip Rand.uniform.sample(gnmf.rowsV*gnmf.colsV) filter { x => x._2 < gnmf.sparsity } map { x
    => Entry(x._1._1, x._1._2, gaussian.draw())}

    sc.parallelize(entries)
  }

  def getW(gnmf: GNMFConfiguration) = {
    val ids = 0 until gnmf.rowsV
    sc.parallelize(ids) map { id => (id, DenseVector.rand[Double](gnmf.k, Gaussian(0,1)))}
  }

  def getH(gnmf: GNMFConfiguration) = {
    val ids = 0 until gnmf.colsV
    sc.parallelize(ids) map { id => (id, DenseVector.rand[Double](gnmf.k, Gaussian(0,1)))}
  }

  def getInformation: String = {
    "# GNMF Benchmark Spark"
  }

  def runSparkBenchmark(runtimeConfig: RuntimeConfiguration, data: Map[String, String]):Unit = {
    execute(runtimeConfig, getGNMFConfiguration(data))
  }
}
