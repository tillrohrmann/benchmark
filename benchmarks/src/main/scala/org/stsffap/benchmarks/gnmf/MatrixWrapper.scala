package org.stsffap.benchmarks.gnmf

import java.io.IOException

import breeze.linalg.DenseMatrix
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value

class MatrixWrapper(var matrix: DenseMatrix[Double]) extends Value {
  def this() = this(null)

  @throws(classOf[IOException])
  def write(out: DataOutputView){
    out.writeInt(matrix.rows)
    out.writeInt(matrix.cols)

    for(row <- 0 until matrix.rows; col <- 0 until matrix.cols){
      out.writeDouble(matrix(row, col))
    }
  }

  def read(in: DataInputView){
    val rows = in.readInt()
    val cols = in.readInt()

    matrix = new DenseMatrix[Double](rows, cols)

    for(row <- 0 until rows; col <- 0 until cols){
      val value = in.readDouble()
      matrix(row, col) = value
    }
  }
}

object MatrixWrapper{
  def apply(matrix: DenseMatrix[Double]) = {
    new MatrixWrapper(matrix)
  }
}
