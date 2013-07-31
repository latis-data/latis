package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.DoubleValue

//class Real(metadata: Metadata, data: Data) extends Scalar(metadata, data) with Number {
class Real extends Scalar with Number {
  //TODO: enforce NumericData, but value class can't extend it
  
  def toDouble = data.doubleValue //TODO: get Option?
  
  
  def compare(that: Double): Int = this.toDouble.compareTo(that)
  //TODO: add epsilon for equality?
  //TODO: compare(Real), units...
    
  def compare(that: String): Int = compare(that.toDouble)
  
}



object Real {
  
  def apply(): Real = new Real //(EmptyMetadata, EmptyData)
  
  def apply(md: Metadata): Real = {
    val r = new Real
    r._metadata = md
    r
  }
  
  def apply(md: Metadata, vs: Seq[Double]): Real = {
    val r = new Real
    r._metadata = md
    r._data = Data(vs)
    r
  }
  
  def apply(md: Metadata, v: Double): Real = {
    val r = new Real
    r._metadata = md
    r._data = DoubleValue(v)
    r
  }
  
  def apply(name: String): Real = {
    //new Real(Metadata(name), EmptyData)
    val r = new Real
    r._metadata = Metadata(name)
    r
  }
  
  def apply(v: Double): Real = {
    //new Real(EmptyMetadata, DoubleValue(v))
    val r = new Real
    r._data = DoubleValue(v)
    r
  }
  
  def apply(name: String, v: Double): Real = {
    //new Real(Metadata(name), DoubleValue(v))
    val r = new Real
    r._metadata = Metadata(name)
    r._data = DoubleValue(v)
    r
  }
  
  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
  def apply(vs: Seq[Double]): Real = {
    //new Real(EmptyMetadata, Data(vs))
    val r = new Real
    r._data = Data(vs)
    r
  }
  
  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
  def apply(name: String, vs: Seq[Double]): Real = {
    //new Real(EmptyMetadata, Data(vs))
    val r = new Real
    r._metadata = Metadata(name)
    r._data = Data(vs)
    r
  }
  
  //def unapply(real: Real): Option[Double] = Some(real.data.doubleValue)
  //use Number to expose double
}