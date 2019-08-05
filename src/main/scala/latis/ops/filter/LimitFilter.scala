package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.metadata.Metadata
import latis.dm.WrappedFunction
import latis.util.iterator.MappingIterator
import latis.util.LatisServiceException

/**
 * Keep only the first 'limit' samples of any outer Function in the Dataset.
 */
class LimitFilter(val limit: Int) extends Filter {
  
  override def applyToFunction(function: Function): Option[Function] = {
    //Assume we can hold this all in memory.
    
    //get the first 'limit' samples, or all if we had less
    val samples = function.iterator.take(limit).toList
    
    //change length of Function in metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> samples.length.toString))
    
    //make the new function with the updated metadata
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(samples, md))
    }
  }
}

object LimitFilter extends OperationFactory {
  
  override def apply(args: Seq[String]): LimitFilter = {
    if (args.length > 1) throw new LatisServiceException("The LimitFilter accepts only one argument")
    try {
      LimitFilter(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new LatisServiceException("The LimitFilter requires an integer argument")
    }
  }
    
  def apply(limit: Int): LimitFilter = new LimitFilter(limit)

  def unapply(lf: LimitFilter): Option[Int] = Some(lf.limit)
}
