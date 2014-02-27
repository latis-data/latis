package latis.dm

import latis.data._
import latis.data.value._
import latis.metadata._

abstract class AbstractScalar(metadata: Metadata = EmptyMetadata, data: Data = EmptyData) 
  extends AbstractVariable(metadata, data) with Scalar {
  
  override def compare(that: String): Int = getData match {
    //note, pattern matching instantiates value classes
    case DoubleValue(d) => d compare that.toDouble
    case LongValue(l) => l compare that.toLong
    case IndexValue(i) => i compare that.toInt
    case StringValue(s) => s compare that
    //TODO: what about Buffer, SeqData?
    //TODO: handle format errors
  }
  
  def getValue: Any = getData match {
    case DoubleValue(d) => d
    case LongValue(l) => l
    case IndexValue(i) => i
    case StringValue(s) => s.trim //TODO: match any TextData?
  }
}