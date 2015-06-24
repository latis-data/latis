package latis.dm

import latis.data.Data
import latis.metadata.Metadata

/**
 * Trait for Scalars representing real (double) data values.
 */
trait Real extends Scalar with Number


object Real {
  def apply(): Real = new AbstractScalar() with Real
  
  def fromDouble(v: Double): Real = new AbstractScalar(data = Data(v)) with Real
  
  def apply(v: AnyVal): Real = v match {
    case d: Double => Real.fromDouble(d)
    case i: Int    => Real.fromDouble(i.toDouble)
    case l: Long   => Real.fromDouble(l.toDouble)
    case f: Float  => Real.fromDouble(f.toDouble)
    case s: Short  => Real.fromDouble(s.toDouble)
    case st: scala.collection.immutable.StringOps => Real.fromDouble(st.toDouble)
  }
  
  def apply(md: Metadata, data: Data): Real = new AbstractScalar(md, data) with Real
  
  def apply(md: Metadata): Real = new AbstractScalar(md) with Real

  def apply(md: Metadata, v: Double): Real = new AbstractScalar(md, Data(v)) with Real
  
  /**
   * Expose the double value represented by this Variable.
   */
  def unapply(real: Real): Option[Double] = Some(real.doubleValue)
  
}