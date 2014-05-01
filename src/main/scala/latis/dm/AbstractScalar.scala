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
  
  /**
   * Try to parse the given string into the appropriate primitive type for this Scalar.
   * If there is a parse exception, return the fill value, missing value, or NaN
   * or throw an error if there is no such value defined.
   */
  override def stringToValue(string: String): Any = {
    try {
      _stringToValue(string)
    } catch {
      case _: NumberFormatException => {
        getFillValue
      }
    }
  }

  /**
   * Based on the CF convention.
   */
  def getMissingValue: Any = getMetadata("missing_value") match {
    case Some(s) => _stringToValue(s) //TODO: handle parsing exception
    case None => this match {
      case _: Real => Double.NaN
      case _ => throw new Error("No default missing value.") //TODO: getType?
    }
  }
  
  /**
   * Based on the CF convention.
   * Default to missing_value, or NaN
   */
  def getFillValue: Any = getMetadata("_FillValue") match {
    case Some(s) => _stringToValue(s) //TODO: handle parsing exception
    case None => getMissingValue
  }
  
  private def _stringToValue(string: String): Any = this match {
    //TODO: "NaN" works for real, make it (and "null"?) work for other types?
    case _:Index   => string.trim.toInt
    case _:Integer => string.trim.toLong
    case _:Real    => string.trim.toDouble
    case _:Text    => string
    //TODO: Time
  } 
}