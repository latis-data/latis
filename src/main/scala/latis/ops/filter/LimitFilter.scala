package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.metadata.Metadata

/**
 * Keep only the first 'limit' samples of any outer Function in the Dataset.
 */
class LimitFilter(val limit: Int) extends Filter {
  
  override def applyToFunction(function: Function) = {
    //set the new length in the metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> limit.toString))

    val it = function.iterator.take(limit)
    Some(Function(function.getDomain, function.getRange, it, md))
  }

}

object LimitFilter extends OperationFactory {
  
  override def apply(args: Seq[String]): LimitFilter = {
    if (args.length > 1) throw new UnsupportedOperationException("The LimitFilter accepts only one argument")
    try {
      LimitFilter(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The LimitFilter requires an integer argument")
    }
  }
    
  def apply(limit: Int): LimitFilter = new LimitFilter(limit)
}
