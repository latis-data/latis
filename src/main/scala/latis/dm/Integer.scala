package latis.dm

import latis.data._
import latis.metadata.VariableMetadata
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata
import latis.data.value.LongValue

trait Integer extends Scalar with Number {
  //def value: Long = longValue
  //def stringToValue(s: String): Long = s.toLong
  //def compare(that: String): Int = longValue compare that.toLong
}


object Integer {
  
//  def apply(): Integer = new Variable() with Integer 
  
  def apply(md: Metadata): Integer = new Variable2(md) with Integer

  def apply(md: Metadata, v: Long): Integer = new Variable2(md, Data(v)) with Integer
  def apply(md: Metadata, vs: Seq[Long]): Integer = new Variable2(md, Data(vs)) with Integer

  def apply(v: Long): Integer = new Variable2(data = Data(v)) with Integer
  def apply(vs: Seq[Long]): Integer = new Variable2(data = Data(vs)) with Integer
    
//  def apply(name: String): Integer = {
//    //new Integer(Metadata(name), EmptyData)
//    val r = new Integer
//    r._metadata = Metadata(name)
//    r
//  }
//  
//  def apply(name: String, v: Long): Integer = {
//    //new Integer(Metadata(name), LongValue(v))
//    val r = new Integer
//    r._metadata = Metadata(name)
//    r._data = LongValue(v)
//    r
//  }
//  
//  //TODO: IndexFunction?, special constructor so this only works in the context of Functions (e.g. domain, range)?
//  def apply(name: String, vs: Seq[Long]): Integer = {
//    //new Integer(EmptyMetadata, Data(vs))
//    val r = new Integer
//    r._metadata = Metadata(name)
//    r._data = Data(vs)
//    r
//  }
  
  
  def unapply(int: Integer): Option[Long] = Some(int.getNumberData.longValue)
}