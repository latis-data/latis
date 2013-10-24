package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.value.DoubleValue

trait Real extends Scalar with Number {
  //def value: Double = doubleValue
  //def stringToValue(s: String): Double = s.toDouble
  def compare(that: String): Int = doubleValue compare that.toDouble
}


object Real {
  implicit def stringToMetadata(name: String): Metadata = Metadata(name) //Metadata with "name" property
  
  def apply(): Real = new Variable2 with Real
  
  def apply(v: Double): Real = new Variable2(data = Data(v)) with Real
  
  def apply(vs: Seq[Double]): Real = new Variable2(data = Data(vs)) with Real
  
  def apply(md: Metadata): Real = new Variable2(md) with Real

  def apply(md: Metadata, v: Double): Real = new Variable2(md, Data(v)) with Real
  
  def apply(md: Metadata, vs: Seq[Double]): Real = new Variable2(md, Data(vs)) with Real

  def unapply(real: Real): Option[Double] = Some(real.getNumberData.doubleValue)
}