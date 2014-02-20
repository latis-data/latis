package latis.dm

import latis.data.Data
import latis.data.value._
import latis.metadata.Metadata
import latis.util.RegEx
import latis.time.Time
import latis.metadata.EmptyMetadata
import latis.data.EmptyData

trait Scalar extends Variable {
  
  //move to AbstractScalar since mixing in Time with Text means that anything here overrides Time?
  //note, we tried overriding this in subclasses but ran into inheritance trouble with "new Time with Real"
  def compare(that: String): Int
  
  def getValue: Any 
    
}

object Scalar {
  
  def apply(vtype: String, metadata: Metadata = EmptyMetadata, data: Data = EmptyData): Scalar = vtype match {
    case "real"    => new AbstractScalar(metadata, data) with Real
    case "integer" => new AbstractScalar(metadata, data) with Integer
    case "text"    => new AbstractScalar(metadata, data) with Text
    case "time"    => Time(metadata, data) //TODO: if type text, set default length=23? or get from 'format'
    case "binary"  => new AbstractScalar(metadata, data) with Binary
    //TODO: vtype = class name, dynamicly construct 
    case _ => ???
  }
  
  def apply(value: AnyVal): Scalar = value match {
    case d: Double => Real(d)
    case f: Float  => Real(f)
    case l: Long   => Integer(l)
    case i: Int    => Integer(i)
    case _ => throw new Error("Unable to make Scalar from value: " + value)
  }
  
  def apply(value: String) = Text(value)
  
  def fromAny(value: Any): Scalar = value match {
    //Note, can't match on AnyVal so we need to repeat these
    case d: Double => Real(d)
    case f: Float  => Real(f)
    case l: Long   => Integer(l)
    case i: Int    => Integer(i)
    case s: String => Text(s)
    case _ => throw new Error("Unable to make Scalar from value: " + value)
  }
  
  def unapply(s: Scalar) = Some(s.getValue)
}



