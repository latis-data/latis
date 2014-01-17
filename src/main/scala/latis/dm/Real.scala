package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.value.DoubleValue

trait Real extends Scalar with Number


object Real {
  //implicit def stringToMetadata(name: String): Metadata = Metadata(name) //Metadata with "name" property
  
  def apply(): Real = new AbstractScalar with Real
  
  def apply(v: Double): Real = new AbstractScalar(data = Data(v)) with Real
  def apply(v: AnyVal): Real = v match {
    case i: Int   => Real(i.toDouble)
    case l: Long  => Real(l.toDouble)
    case f: Float => Real(f.toDouble)
    case s: Short => Real(s.toDouble)
  }
  
  def apply(vs: Seq[Double]): Real = new AbstractScalar(data = Data(vs)) with Real
  
  def apply(md: Metadata): Real = new AbstractScalar(md) with Real

  //TODO: review consistency in order or args
  def apply(md: Metadata, v: Double): Real = new AbstractScalar(md, Data(v)) with Real
  
  def apply(md: Metadata, vs: Seq[Double]): Real = new AbstractScalar(md, Data(vs)) with Real

  def unapply(real: Real): Option[Double] = Some(real.getNumberData.doubleValue)
  
  
  //def apply(name: String): Real = new AbstractScalar(Metadata(name)) with Real
  //def apply(name: String, v: Double): Real = new AbstractScalar(Metadata(name), Data(v)) with Real
  //def apply(name: String, vs: Seq[Double]): Real = new AbstractScalar(Metadata(name), Data(vs)) with Real
}