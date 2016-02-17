package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.metadata.Metadata

/**
 * Return the sample containing the min scalar of any outer Function in the Dataset.
 */
class MinFilter(name: String) extends Filter {
  override def applyToFunction(function: Function) = {
    //get the samples from the iterator
    val samples = function.iterator.toList 
    
    //change length of Function in metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> samples.length.toString))
    
    //make the new function with the updated metadata
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(samples, md))
    }
  }
}

object MinFilter extends OperationFactory {
  def apply(name: String): MinFilter = new MinFilter(name)
}
