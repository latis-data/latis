package latis.dm

import latis.data.Data
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
  
  /**
   * Expose the long value represented by this Variable.
   */
  def unapply(int: Integer): Option[Long] = Some(int.getNumberData.longValue)
}