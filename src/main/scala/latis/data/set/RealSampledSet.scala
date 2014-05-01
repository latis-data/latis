package latis.data.set

import scala.collection.immutable
import latis.data.value.DoubleValue
import latis.data.Data

class RealSampledSet(values: immutable.Seq[Double]) extends DomainSet {

  def apply(index: Int): Data = DoubleValue(values(index))
  
  def indexOf(data: Data): Int = data match {
    case DoubleValue(d) => values.indexOf(d)
    //TODO: other Data that represent Doubles?
  }
  
  def recordSize: Int = 8
  
  def iterator: Iterator[Data] = values.iterator.map(DoubleValue(_))
}

object RealSampledSet {
  
  def apply(ds: Seq[Double]) = new RealSampledSet(ds.toIndexedSeq)
}