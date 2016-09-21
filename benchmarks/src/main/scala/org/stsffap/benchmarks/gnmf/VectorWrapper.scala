package org.stsffap.benchmarks.gnmf

import java.io.IOException

import breeze.linalg.DenseVector
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Value

class VectorWrapper(var vector: DenseVector[Double]) extends Value {
  def this() = this(null)

  @throws(classOf[IOException])
  def write(out: DataOutputView){
    out.writeInt(vector.length)

    for(value <- vector.valuesIterator){
      out.writeDouble(value)
    }
  }

  @throws(classOf[IOException])
  def read(in: DataInputView){
    val length = in.readInt()

    val data = new Array[Double](length)

    for(idx <- 0 until length){
      val value = in.readDouble()
      data(idx) = value
    }

    vector = new DenseVector[Double](data)
  }


  override def toString() = {
    vector.toString()
  }
}

object VectorWrapper{
  def apply(vector: DenseVector[Double]) = { new VectorWrapper(vector) }
}
