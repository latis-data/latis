package latis.data.set

import scala.collection.immutable
import latis.data.value.DoubleValue
import latis.data.Data
import latis.data.value.LongValue
import latis.data.value.StringValue

class TextSampledSet(values: immutable.Seq[String]) extends DomainSet {
  //TODO: factor out SampledSet?
  //TODO: assert that these are ordered, could be formatted times

  def apply(index: Int): Data = StringValue(values(index))
  
  def indexOf(data: Data): Int = data match {
    case StringValue(s) => values.indexOf(s)
  }
  
  //max length of values
  //def recordSize: Int = values.foldRight(0)((s,m) => Math.max(s.length, m))
  //assume all the same size, for now
  def recordSize: Int = values.head.length
  
  override def length: Int = values.length
  
  def iterator: Iterator[Data] = values.iterator.map(StringValue(_))
}

object TextSampledSet {
  
  def apply(ds: Seq[String]) = new TextSampledSet(ds.toIndexedSeq)
}