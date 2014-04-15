package latis.ops

import latis.dm.Function

/**
 * Keep only the first 'limit' samples of any outer Function in the Dataset.
 */
class LimitFilter(val limit: Int) extends Filter {
  
  /**
   * Make a new Function with the original Function's types and limited iterator.
   */
  override def filterFunction(function: Function) = {
    //TODO: consider iterable once issues
    Some(Function(function.getDomain, function.getRange, function.iterator.take(limit)))
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
