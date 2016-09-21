package org.stsffap.benchmarks.gnmf

import breeze.linalg.DenseVector
import breeze.stats.distributions.{Gaussian, Rand}
import org.apache.flink.api.scala._
import org.stsffap.benchmarks.{RuntimeConfiguration, FlinkBenchmark}

@SerialVersionUID(1L)
class GNMFFlink(@transient val env: ExecutionEnvironment, val parallelism: Int) extends FlinkBenchmark with
GNMFBenchmark with Serializable {
  var gnmf: GNMFConfiguration = null
  @transient var V: DataSet[Entry]= null

  def getScalaPlan(runtimeConfiguration: RuntimeConfiguration, gnmfConfig: GNMFConfiguration): Unit = {
    gnmf = gnmfConfig

    V = getV
    val initialW = getW
    val initialH = getH

    val initialWH = (initialW map {
      x =>
        (0, x)
    }) union (initialH map {
      x=>
        (1, x)
    })

    val stepWH = (WH: DataSet[(Int, (Int, VectorWrapper))]) => {
      val W = WH filter { x => x._1 == 0} map {
        x =>
          x._2
      }
      val H = WH filter { x => x._1 == 1} map {
        x =>
          x._2
      }

      //X = W' * V
      val partialX = V join W where {x => x.row} equalTo { x => x._1 } apply { (entry, right) =>
        val (_, vec) = right
        (entry.col, VectorWrapper(vec.vector * entry.value))
      }

      val X = partialX.groupBy{x=>x._1}.reduce((a, b) => (a._1, VectorWrapper(a._2.vector + b._2.vector)))

      //Y = W'*W*H
      val partialWW = W map { input =>
        val (idx, vec) = input
        MatrixWrapper(vec.vector * vec.vector.t)}

      val WW = partialWW.reduce((a, b) => MatrixWrapper(a.matrix + b.matrix))

      val Y = H cross WW apply { (left, mw) =>
        val (idx, colvw) = left
        (idx, VectorWrapper(mw.matrix * colvw.vector))}

      //H <- H .* X ./ Y
      val XY = X join Y where {x => x._1} equalTo { x => x._1 } apply { (left, right) =>
        val (idx, xw) = left
        val yw = right._2
        (idx, VectorWrapper(xw.vector :/ yw.vector))
      }
      val nH = H join XY where { x => x._1 } equalTo { x => x._1 } apply { (left, right) =>
        val (idx, hw) = left
        val xyw = right._2
        (idx, VectorWrapper(hw.vector :* xyw.vector))
      }

      // T = V*H'
      val partialT = V join nH where { x=> x.col } equalTo {x => x._1 } apply { (entry, right) =>
        val vw = right._2
        (entry.row, VectorWrapper(vw.vector * entry.value))
      }
      val T = partialT.groupBy{x => x._1}.reduce((a, b) => (a._1, VectorWrapper(a._2.vector + b._2.vector)))

      // U = W*H*H'
      val partialHH = nH map { input =>
        val vw = input._2
        MatrixWrapper(vw.vector * vw.vector.t)}

      val HH = partialHH.reduce((a, b) => MatrixWrapper(a.matrix + b.matrix))

      val U = W cross HH apply { (left, mw) =>
        val (idx, vw) = left
        (idx, VectorWrapper(mw.matrix * vw.vector))
      }

      // W <- W.*T./U
      val TU = T join U where (x => x._1) equalTo ( x => x._1 ) apply { (left, right) =>
        val (idx, tw) = left
        val uw = right._2

        (idx, VectorWrapper(tw.vector :/ uw.vector))
      }

      val nW = W join TU where ( x => x._1 ) equalTo ( x=> x._1) apply { (left, right) =>
        val (idx, ww) = left
        val tuw = right._2

        (idx, VectorWrapper(ww.vector :* tuw.vector))
      }

      val unionWH = (nW map {
        x =>
          (0,x)
      }) union (nH map {
        x =>
          (1,x)})
      unionWH map { x => x }
    }

    val resultingWH = initialWH.iterate(gnmf.maxIterations)(stepWH)

    val resultingW = resultingWH.filter(x => x._1 == 0).map{ x => (x._2._1, x._2._2.vector.activeSize) }
    val resultingH = resultingWH.filter(x => x._1 == 1).map{ x => (x._2._1, x._2._2.vector.activeSize) }

    val outputW = runtimeConfiguration.outputPath.endsWith("/") match {
      case true => runtimeConfiguration.outputPath + "W"
      case false => runtimeConfiguration.outputPath + "/W"
    }

    val outputH = runtimeConfiguration.outputPath.endsWith("/") match {
      case true => runtimeConfiguration.outputPath + "H"
      case false => runtimeConfiguration.outputPath + "/H"
    }

    resultingW.writeAsCsv(outputW)
    resultingW.writeAsCsv(outputH)
  }

  def getV = {
    val rows = 0 until gnmf.rowsV
    env.fromCollection(rows).groupBy(x=>x).reduceGroup(xs => xs.next).flatMap{ row =>
      val gaussian = new Gaussian(0,1)
      Rand.uniform.sample(gnmf.colsV).zipWithIndex.filter(x => x._1 < gnmf.sparsity).map{ x =>
        Entry(row, x._2, gaussian.draw())
      }.toIterator
    }

  }

  def getW = {
    val idxs = 0 until gnmf.rowsV

    env.fromCollection(idxs).groupBy(x => x).reduceGroup(xs => xs.next).map{ idx => (idx,
      VectorWrapper(DenseVector.rand(gnmf.k,new Gaussian(0,1))))}
  }

  def getH = {
    val idxs = 0 until gnmf.colsV

    env.fromCollection(idxs).groupBy(x => x).reduceGroup(xs => xs.next).map{ idx => (idx,
      VectorWrapper(DenseVector.rand(gnmf.k,new Gaussian(0,1))))}
  }

  def createProgram(runtimeConfiguration: RuntimeConfiguration, data: Map[String, String]): Unit = {
    getScalaPlan(runtimeConfiguration, getGNMFConfiguration(data))
  }

  def getInformation: String = {
    "# GNMF Benchmark Stratosphere"
  }
}
