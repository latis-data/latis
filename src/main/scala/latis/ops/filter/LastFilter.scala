package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory

/**
 * Keep only the last sample of any outer Function in the Dataset.
 */
class LastFilter extends Filter {
  //TODO: could we return the Sample not nested within the Function (i.e. reduced)?
  override def applyToFunction(function: Function) = {
    val s = function.iterator.drop(function.getLength-1).next
    Some(Function(Seq(s)))
  }
}

object LastFilter extends OperationFactory {
  override def apply() = new LastFilter
}