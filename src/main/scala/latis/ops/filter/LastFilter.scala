package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.metadata.Metadata

/**
 * Keep only the last sample of any outer Function in the Dataset.
 */
class LastFilter extends Filter {

  override def applyToFunction(function: Function) = {
    //get the last sample, or none if empty, as List
    val samples = function.iterator match {
      case it: Iterator[Sample] if (it.nonEmpty) => List(it.reduceLeft((_,e) => e))  //keep last sample
      case _ => List[Sample]()  //empty
    }
    
    //change length of Function in metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> samples.length.toString))
    
    //make the new function with the updated metadata
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case 1 => Some(Function(samples, md))
    }
  }
}

object LastFilter extends OperationFactory {
  override def apply() = new LastFilter
}