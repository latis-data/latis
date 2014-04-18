package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory

/**
 * Keep only the last sample of any outer Function in the Dataset.
 */
class LastFilter extends Filter {
  override def applyToFunction(function: Function) = Some(Function(Seq(function.getLastSample)))
}

object LastFilter extends OperationFactory {
  override def apply() = new LastFilter
}