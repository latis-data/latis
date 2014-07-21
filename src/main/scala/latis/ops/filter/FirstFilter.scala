package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.metadata.Metadata

/**
 * Keep only the first sample of any outer Function in the Dataset.
 */
class FirstFilter extends Filter {
  override def applyToFunction(function: Function) = {
    //get the first sample, or none if empty, as List
    val samples = function.iterator.take(1).toList //0 or 1
    
    //change length of Function in metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> samples.length.toString))
    
    //make the new function with the updated metadata
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, md)) //empty Function with type of original
      case 1 => Some(Function(samples, md))
    }
  }
}

object FirstFilter extends OperationFactory {
  override def apply(): FirstFilter = new FirstFilter
}
