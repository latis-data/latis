package latis.dm

import latis.data.Data
import latis.data.value._
import latis.metadata.Metadata
import latis.util.RegEx

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
//}