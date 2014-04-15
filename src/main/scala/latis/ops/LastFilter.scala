package latis.ops

import latis.dm.Function

/**
 * Keep only the last sample of any outer Function in the Dataset.
 */
class LastFilter extends Filter {
  override def filterFunction(function: Function) = Some(Function(Seq(function.getLastSample)))
}

object LastFilter extends OperationFactory {
  override def apply() = new LastFilter
}