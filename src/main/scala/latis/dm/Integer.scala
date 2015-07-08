package latis.dm

import latis.data.Data
import latis.data.value.LongValue
import latis.metadata.Metadata

/**
 * Trait for Scalars representing integer (long) data values.
 */
trait Integer extends Scalar with Number


object Integer {
  
  def apply(md: Metadata): Integer = new AbstractScalar(md) with Integer

  def apply(md: Metadata, data: Data): Integer = new AbstractScalar(md, data) with Integer
  def apply(md: Metadata, v: Long): Integer = new AbstractScalar(md, Data(v)) with Integer

  def apply(v: Long): Integer = new AbstractScalar(data = Data(v)) with Integer
  def apply(v: AnyVal): Integer = v match {
    case i: Int    => Integer(i.toLong)
    case d: Double => Integer(d.toLong)
    case f: Float  => Integer(f.toLong)
    case s: Short  => Integer(s.toLong)
    case st: scala.collection.immutable.StringOps => Integer(st.toLong)
  }
    
  def apply(md: Metadata, v: Any): Integer = v match {
    case l: Long => new AbstractScalar(md, data = LongValue(l)) with Integer
    case lv: LongValue => new AbstractScalar(md, lv) with Integer
    case d: Double => Integer(md,d.toLong)
    case i: Int    => Integer(md,i.toLong)
    case f: Float  => Integer(md,f.toLong)
    case s: Short  => Integer(md,s.toLong)
    case st: scala.collection.immutable.StringOps => Integer(md,st.toLong)
  }
    
  /**
   * Expose the long value represented by this Variable.
   */
  def unapply(int: Integer): Option[Long] = Some(int.longValue)
}