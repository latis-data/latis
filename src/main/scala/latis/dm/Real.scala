package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.value.DoubleValue

trait Real extends Scalar[Double] with Number {
  //TODO: enforce NumericData, but type check requires instance of value class
  
  //def doubleValue = data.asInstanceOf[DoubleValue].doubleValue //TODO: will this require instance of value class?
  //TODO: get Option?
  //TODO: what about DoubleValue vs SeqData?
  //  pattern match but want to keep benefits of value class
}


object Real {
  
//  def apply(): Real = new Real //(EmptyMetadata, EmptyData)
  
  def apply(md: Metadata): Real = new Variable2(md) with Real

  
//  def apply(md: Metadata, vs: Seq[Double]): Real = {
//    val r = new Real
//    r._metadata = md
//    r._data = Data(vs)
//    r
//  }
  
  def apply(md: Metadata, v: Double): Real = new Variable2(md, DoubleValue(v)) with Real
  
//  def apply(name: String): Real = {
//    //new Real(Metadata(name), EmptyData)
//    val r = new Real
//    r._metadata = Metadata(name)
//    r
//  }
  
  def apply(v: Double): Real = new Variable2(data = DoubleValue(v)) with Real
//TODO: do we need a RealVariable to make pattern matching work?

//  
//  def apply(name: String, v: Double): Real = {
//    //new Real(Metadata(name), DoubleValue(v))
//    val r = new Real
//    r._metadata = Metadata(name)
//    r._data = DoubleValue(v)
//    r
//  }
//  
//  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
//  def apply(vs: Seq[Double]): Real = {
//    //new Real(EmptyMetadata, Data(vs))
//    val r = new Real
//    r._data = Data(vs)
//    r
//  }
//  
//  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
//  def apply(name: String, vs: Seq[Double]): Real = {
//    //new Real(EmptyMetadata, Data(vs))
//    val r = new Real
//    r._metadata = Metadata(name)
//    r._data = Data(vs)
//    r
//  }
//  
//  def unapply(real: Real): Option[Double] = Some(real.doubleValue)
//  //use Number to expose double
}