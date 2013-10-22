package latis.dm

import latis.data.Data
import latis.metadata.Metadata

trait Scalar[A] extends Variable with Ordered[A] { 
  def value: A
  
  def compare(that: A): Int
  
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

//object Scalar {
//  //handy for ASCII Writers
//  def unapply(s: Scalar) = s match {
//    case Number(n) => Some(n.toString)
//    case Text(t) => Some(t)
//  }
//}