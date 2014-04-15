package latis.ops

import latis.dm.Function

/**
 * Keep only the first sample of any outer Function in the Dataset.
 */
class FirstFilter extends Filter {
  override def filterFunction(function: Function) = Some(Function(Seq(function.getFirstSample)))
}

object FirstFilter extends OperationFactory {
  override def apply(): FirstFilter = new FirstFilter
}
