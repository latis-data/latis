package latis.dm

import latis.data.Data
import latis.data.value._
import latis.metadata.Metadata
import latis.util.RegEx
import latis.time.Time
import latis.metadata.EmptyMetadata
import latis.data.EmptyData

trait Scalar extends Variable {
//trait Scalar[A] extends Variable { //TODO: with Ordered[Scalar[A]] { 
  //def value: A  
  
  //def compare(that: Scalar[B]): Int =
  
  //note, we tried overriding this in subclasses but ran into inheritance trouble with "new Time with Real"
  def compare(that: String): Int = getData match {
    //note, pattern matching instantiates value classes
    case DoubleValue(d) => d compare that.toDouble
    case LongValue(l) => l compare that.toLong
    case IndexValue(i) => i compare that.toInt
    case StringValue(s) => s compare that
    //TODO: what about Buffer, SeqData?
    //TODO: handle format errors
  }
  
  def getValue: Any = getData match {
    case DoubleValue(d) => d
    case LongValue(l) => l
    case IndexValue(i) => i
    case StringValue(s) => s.trim
  }
  
  //convert the string to a value of our type (e.g. for comparison)
  //def stringToValue(s: String): A
  
    //deal with ISO formatted time
    //TODO: do conversion later, as needed?
    //if (vname == time) 
    //TSDS delegates to Variable to parse value into double for comparison
    //but we don't want to have to do that for each call to filterScalar?
    //  but we do value.toDouble already
    //  but need to convert units, need Time variable
    //  does it still make sense to delegate to Variable?
    //  it's one thing to convert its own value, but as a converter for others?
    
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
  
//  def apply(vtype: String, metadata: Metadata): Scalar = Scalar(vtype, metadata)
//  : Scalar = vtype match {
//    case "real"    => Real(metadata)
//    case "integer" => Integer(metadata)
//    case "text"    => Text(metadata)
//    case "time"    => Time(metadata) //TODO: if type text, set default length=23? or get from 'format'
//    case "binary"  => Binary(metadata)
//    //TODO: vtype = class name, dynamicly construct 
//    case _ => ???
//  }
  
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



