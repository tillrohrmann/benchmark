package org.stsffap.benchmarks.gnmf

import java.io.{DataInput, IOException, DataOutput}

import breeze.linalg.DenseVector
import eu.stratosphere.types.Value

class VectorWrapper(var vector: DenseVector[Double]) extends Value {
  def this() = this(null)

  @throws(classOf[IOException])
  def write(out: DataOutput){
    out.writeInt(vector.length)

    for(value <- vector.valuesIterator){
      out.writeDouble(value)
    }
  }

  @throws(classOf[IOException])
  def read(in: DataInput){
    val length = in.readInt()

    val data = new Array[Double](length)

    for(idx <- 0 until length){
      val value = in.readDouble()
      data(idx) = value
    }

    vector = new DenseVector[Double](data)
  }
}

object VectorWrapper{
  def apply(vector: DenseVector[Double]) = { new VectorWrapper(vector) }
}
