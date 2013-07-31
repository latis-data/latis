package latis.dm

import latis.data.Data
import latis.metadata.Metadata

//abstract class Scalar(metadata: Metadata, data: Data) extends Variable(metadata, data) {
abstract class Scalar extends Variable { //TODO: with Ordered or Ordering? {
  
  //def compare(that: AnyVal): Int
  def compare(that: Double): Int
  def compare(that: String): Int
  
  
    //deal with ISO formatted time
    //TODO: do conversion later, as needed?
    //if (vname == time) 
    //TSDS delegates to Variable to parse value into double for comparison
    //but we don't want to have to do that for each call to filterScalar?
    //  but we do value.toDouble already
    //  but need to convert units, need Time variable
    //  does it still make sense to delegate to Variable?
    //  it's one thing to convert its own value, but as a converter for others?
    //could we do more with Variables being Comparible?
    //  instead of assuming doubles
    //  needed for text
    //but compares only to same type?
    //  need to compare to AnyVal?
    
    
}

object Scalar {
  
  def unapply(s: Scalar) = s match {
    case Number(n) => Some(n)
    case Text(t) => Some(t)
  }
}