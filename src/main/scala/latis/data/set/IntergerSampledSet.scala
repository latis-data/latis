package latis.data.set

import scala.collection.immutable
import latis.data.value.DoubleValue
import latis.data.Data
import latis.data.value.LongValue

class IntegerSampledSet(values: immutable.Seq[Long]) extends DomainSet {
  //TODO: factor out SampledSet?

  def apply(index: Int): Data = LongValue(values(index))
  
  def indexOf(data: Data): Int = data match {
    case LongValue(d) => values.indexOf(d)
  }
  
  def recordSize: Int = 8
  
  override def length: Int = values.length
  
  def iterator: Iterator[Data] = values.iterator.map(LongValue(_))
}

object IntegerSampledSet {
  
  def apply(ds: Seq[Long]) = new IntegerSampledSet(ds.toIndexedSeq)
}