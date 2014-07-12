package org.stsffap.benchmarks.gnmf

import breeze.linalg.DenseVector
import breeze.stats.distributions.Gaussian
import eu.stratosphere.api.common.{Plan, PlanExecutor}
import eu.stratosphere.api.scala.operators.CsvOutputFormat
import eu.stratosphere.api.scala.{DataSet, CollectionDataSource, ScalaPlan}
import org.stsffap.benchmarks.{RuntimeConfiguration, StratosphereBenchmark}

class GNMFStratosphere(@transient val executor: PlanExecutor, val parallelism: Int) extends StratosphereBenchmark with
GNMFBenchmark with Serializable {
  var gnmf: GNMFConfiguration = null
  @transient var V: DataSet[Entry]= null

  def getScalaPlan(runtimeConfiguration: RuntimeConfiguration, gnmfConfig: GNMFConfiguration): ScalaPlan = {
    gnmf = gnmfConfig

    V = getV
    val initialW = getW
    val initialH = getH

    val initialWH = (initialW map { x => (0, x) }) union (initialH map { x=> (1, x)})

    val stepWH = (WH: DataSet[(Int, (Int, VectorWrapper))]) => {
      val W = WH filter { x => x._1 == 0} map { x => x._2 }
      val H = WH filter { x => x._1 == 1} map { x => x._2 }

      //X = W' * V
      val partialX = V join W where {x => x.row} isEqualTo { x => x._1 } map { case (entry, (_, vec)) =>
        (entry.col, VectorWrapper(vec.vector * entry.value))
      }

      val X = partialX.groupBy{x=>x._1}.combinableReduceGroup{ xs =>
        xs.reduce((a,b) => (a._1, VectorWrapper(a._2.vector + b._2.vector)))
      }

      //Y = W'*W*H
      val partialWW = W map { case (idx, vec) => MatrixWrapper(vec.vector * vec.vector.t)}
      val WW = partialWW.combinableReduceAll( ms =>
        ms.reduce{(a,b) => MatrixWrapper(a.matrix + b.matrix)}
      )

      val Y = H cross WW map { case ((idx, colvw), mw) => (idx, VectorWrapper(mw.matrix * colvw.vector))}

      //H <- H .* X ./ Y
      val XY = X join Y where {x => x._1} isEqualTo { x => x._1 } map { case ((idx, xw), (_, yw)) =>
        (idx, VectorWrapper(xw.vector :/ yw.vector))
      }
      val nH = H join XY where { x => x._1 } isEqualTo { x => x._1 } map { case ((idx, hw), (_, xyw)) =>
        (idx, VectorWrapper(hw.vector :* xyw.vector))
      }

      // T = V*H'
      val partialT = V join nH where { x=> x.col } isEqualTo {x => x._1 } map { case (entry, (_, vw)) =>
        (entry.col, VectorWrapper(vw.vector * entry.value))
      }
      val T = partialT.groupBy{x => x._1}.combinableReduceGroup{ ts =>
        ts.reduce{ (a,b) => (a._1, VectorWrapper(a._2.vector + b._2.vector))}
      }

      // U = W*H*H'
      val partialHH = nH map { case (_, vw) => MatrixWrapper(vw.vector * vw.vector.t)}
      val HH = partialHH.combinableReduceAll(ms =>
        ms.reduce{(a,b) => MatrixWrapper(a.matrix + b.matrix)}
      )

      val U = W cross HH map { case ((idx, vw), mw) =>
        (idx, VectorWrapper(mw.matrix * vw.vector))
      }

      // W <- W.*T./U
      val TU = T join U where (x => x._1) isEqualTo ( x => x._1 ) map { case ((idx, tw), (_, uw)) =>
        (idx, VectorWrapper(tw.vector :/ uw.vector))
      }

      val nW = W join TU where ( x => x._1 ) isEqualTo ( x=> x._1) map { case ((idx, ww), (_, tuw))=>
        (idx, VectorWrapper(ww.vector :* tuw.vector))
      }

      val unionWH = (nW map { x => (0,x)}) union (nH map { x=> (1,x)})
      unionWH map { x => x }
    }

    val resultingWH = initialWH.iterate(gnmf.maxIterations, stepWH)

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
    val sinkW = resultingW.write(outputW, CsvOutputFormat[(Int, Int)]())
    val sinkH = resultingH.write(outputH, CsvOutputFormat[(Int, Int)]())

    new ScalaPlan(Seq(sinkW, sinkH))
  }

  def getV = {
    val gaussian = new Gaussian(0,1)

    val coords = for(row <- 0 until gnmf.rowsV; col <- 0 until gnmf.colsV) yield (row, col)
    val entries = coords zip gaussian.sample(gnmf.rowsV*gnmf.colsV) filter { x => x._2 < gnmf.sparsity } map { case
      (coord, _) => (coord._1*gnmf.colsV+ coord._2)}

    CollectionDataSource(entries).groupBy( x => x).reduceGroup(xs => xs.next).map{
      x =>
        val row = x /gnmf.colsV
        val col = x %gnmf.colsV
        Entry(row, col, new Gaussian(0, 1).draw())
    }
  }

  def getW = {
    val idxs = 0 until gnmf.rowsV

    CollectionDataSource(idxs).groupBy(x => x).reduceGroup(xs => xs.next).map{ idx => (idx,
      VectorWrapper(DenseVector.rand(gnmf.k,new Gaussian(0,1))))}
  }

  def getH = {
    val idxs = 0 until gnmf.colsV

    CollectionDataSource(idxs).groupBy(x => x).reduceGroup(xs => xs.next).map{ idx => (idx,
      VectorWrapper(DenseVector.rand(gnmf.k,new Gaussian(0,1))))}
  }

  def getPlan(runtimeConfiguration: RuntimeConfiguration, data: Map[String, String]): Plan = {
    getScalaPlan(runtimeConfiguration, getGNMFConfiguration(data))
  }

  def getInformation: String = {
    "# GNMF Benchmark Stratosphere"
  }
}
