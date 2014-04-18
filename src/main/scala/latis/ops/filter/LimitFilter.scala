package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample

/**
 * Keep only the first 'limit' samples of any outer Function in the Dataset.
 */
class LimitFilter(val limit: Int) extends Filter {
  
  private var count = 0
  
  /**
   * Only allow this to be applied 'limit' times.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    count += 1
    if (count > limit) None
    else Some(sample)
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
