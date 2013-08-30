package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.value.LongValue

class Integer extends Scalar with Number {
  def doubleValue = data.asInstanceOf[LongValue].doubleValue 
  def longValue = data.asInstanceOf[LongValue].longValue 
}


object Integer {
  
  def apply(): Integer = new Integer //(EmptyMetadata, EmptyData)
  
  def apply(md: Metadata): Integer = {
    val r = new Integer
    r._metadata = md
    r
  }
  
  def apply(md: Metadata, vs: Seq[Long]): Integer = {
    val r = new Integer
    r._metadata = md
    r._data = Data(vs)
    r
  }
  
  def apply(md: Metadata, v: Long): Integer = {
    val r = new Integer
    r._metadata = md
    r._data = LongValue(v)
    r
  }
  
  def apply(name: String): Integer = {
    //new Integer(Metadata(name), EmptyData)
    val r = new Integer
    r._metadata = Metadata(name)
    r
  }
  
  def apply(v: Long): Integer = {
    //new Integer(EmptyMetadata, DoubleValue(v))
    val r = new Integer
    r._data = LongValue(v)
    r
  }
  
  def apply(name: String, v: Long): Integer = {
    //new Integer(Metadata(name), LongValue(v))
    val r = new Integer
    r._metadata = Metadata(name)
    r._data = LongValue(v)
    r
  }
  
  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
  def apply(vs: Seq[Long]): Integer = {
    //new Integer(EmptyMetadata, Data(vs))
    val r = new Integer
    r._data = Data(vs)
    r
  }
  
  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
  def apply(name: String, vs: Seq[Long]): Integer = {
    //new Integer(EmptyMetadata, Data(vs))
    val r = new Integer
    r._metadata = Metadata(name)
    r._data = Data(vs)
    r
  }
  
  
  def unapply(int: Integer): Option[Long] = Some(int.longValue)
}