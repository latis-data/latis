package latis.ops

import latis.dm._
import scala.collection.mutable.{Seq => mSeq}
import scala.collection.mutable.ListBuffer

/**
 * Reduce nested Functions of Index to a single Function of index.
 */
class Flatten extends Operation {
  /*
   * try using Operation2
   * don't rely on domain,range types from Function
   *   but we need to know if we have a Function of Index
   *   
   * Could we make an IndexFunction everywhere that we have a Index domain?
   * how would this relate to having Function[Index,V]? type erasure
   */
  
  override def applyToFunction(function: Function): Option[Variable] = {
    //all nested Functions must have Index domain
    //assume we can fit it all into memory
    
    def accumulateSamples(function: Function, samplesSoFar: ListBuffer[Sample]): Unit = {
      function.iterator.toSeq.foreach(s => s match {
        case Sample(_: Index, f: Function) => accumulateSamples(f, samplesSoFar)
        case Sample(_: Index, v: Variable) => samplesSoFar += s
      })
      
//      val samples = function.iterator.toSeq
//      samples.headOption match {
//      case Some(sample) => sample.domain match {
//        case _: Index => {
//          
//          
//        }
//        
//        case _ => Seq[Sample]() //can only flatten if all Functions have an Index domain
//      }
//      
//      case None => samplesSoFar //function was empty, nothing to accumulate
//    }
    
      //???
    }
    
    val buffer = ListBuffer[Sample]()
    accumulateSamples(function, buffer)
   
    Some(Function(buffer.toSeq))
  }
  
  private def isIndexFunction(function: Function) = function.getDomain.isInstanceOf[Index]
}

object Flatten {
  def apply(): Flatten = new Flatten()
}