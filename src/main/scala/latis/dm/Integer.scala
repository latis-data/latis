package latis.dm

import latis.data.Data
import latis.data.value.LongValue
import latis.metadata.Metadata

/**
 * Trait for Scalars representing integer (long) data values.
 */
trait Integer extends Scalar with Number with Ordering[Integer] {
  
  /**
   * Implement Ordering for Integer Variables so we can sort.
   */
  def compare(x: Integer, y: Integer): Int = (x,y) match {
    case (Integer(a), Integer(b)) => a compare b
  }
  
}


object Integer {
  
  def apply(data: Data): Integer = data match {
    case v: LongValue => new AbstractScalar(data = v) with Integer
    case _ => throw new UnsupportedOperationException("An Integer must be constructed with a LongValue.")
  }
  
  def apply(v: String): Integer = Integer(LongValue(v.toLong))
  def apply(v: Double): Integer = Integer(LongValue(v.toLong))
  def apply(v: Float):  Integer = Integer(LongValue(v.toLong))
  def apply(v: Long):   Integer = Integer(LongValue(v))
  def apply(v: Int):    Integer = Integer(LongValue(v.toLong))
  def apply(v: Short):  Integer = Integer(LongValue(v.toLong))

  
  def apply(md: Metadata, data: Data): Integer = data match {
    case dv: LongValue => new AbstractScalar(md, data) with Integer
    case _ => throw new UnsupportedOperationException("An Integer must be constructed with a LongValue.")
  }
  
  def apply(md: Metadata, v: String): Integer = Integer(md, LongValue(v.toLong))
  def apply(md: Metadata, v: Double): Integer = Integer(md, LongValue(v.toLong))
  def apply(md: Metadata, v: Float):  Integer = Integer(md, LongValue(v.toLong))
  def apply(md: Metadata, v: Long):   Integer = Integer(md, LongValue(v))
  def apply(md: Metadata, v: Int):    Integer = Integer(md, LongValue(v.toLong))
  def apply(md: Metadata, v: Short):  Integer = Integer(md, LongValue(v.toLong))
  
  def apply(md: Metadata): Integer = new AbstractScalar(md) with Integer

  
  /**
   * Expose the long value represented by this Variable.
   */
  def unapply(int: Integer): Option[Long] = Some(int.longValue)
}