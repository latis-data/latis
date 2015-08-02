package latis.dm

import latis.data.Data
import latis.data.value.DoubleValue
import latis.metadata.Metadata

/**
 * Trait for Scalars representing real (double) data values.
 */
trait Real extends Scalar with Number


object Real {
  //needed, empty? def apply(): Real = new AbstractScalar() with Real
  
  def apply(data: Data): Real = data match {
    case dv: DoubleValue => new AbstractScalar(data = dv) with Real
    case _ => throw new UnsupportedOperationException("A Real must be constructed with a DoubleValue.")
  }
  
  def apply(v: String): Real = Real(DoubleValue(v.toDouble))
  def apply(v: Double): Real = Real(DoubleValue(v))
  def apply(v: Float):  Real = Real(DoubleValue(v.toDouble))
  def apply(v: Long):   Real = Real(DoubleValue(v.toDouble))
  def apply(v: Int):    Real = Real(DoubleValue(v.toDouble))
  def apply(v: Short):  Real = Real(DoubleValue(v.toDouble))

  
  def apply(md: Metadata, data: Data): Real = data match {
    case dv: DoubleValue => new AbstractScalar(md, data) with Real
    case _ => throw new UnsupportedOperationException("A Real must be constructed with a DoubleValue.")
  }
  
  def apply(md: Metadata, v: String): Real = Real(md, DoubleValue(v.toDouble))
  def apply(md: Metadata, v: Double): Real = Real(md, DoubleValue(v))
  def apply(md: Metadata, v: Float):  Real = Real(md, DoubleValue(v.toDouble))
  def apply(md: Metadata, v: Long):   Real = Real(md, DoubleValue(v.toDouble))
  def apply(md: Metadata, v: Int):    Real = Real(md, DoubleValue(v.toDouble))
  def apply(md: Metadata, v: Short):  Real = Real(md, DoubleValue(v.toDouble))
  
  def apply(md: Metadata): Real = new AbstractScalar(md) with Real

  
  /**
   * Expose the double value represented by this Variable.
   */
  def unapply(real: Real): Option[Double] = Some(real.doubleValue)
  
}