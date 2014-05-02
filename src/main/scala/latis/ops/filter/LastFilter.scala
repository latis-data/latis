package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory

/**
 * Keep only the last sample of any outer Function in the Dataset.
 */
class LastFilter extends Filter {

  override def applyToFunction(function: Function) = {
    //TODO: IterableOnce problem?
    //val s = function.iterator.drop(function.getLength-1).next
    val s = function.iterator.toList.last
    Some(s)
  }
}

object LastFilter extends OperationFactory {
  override def apply() = new LastFilter
}