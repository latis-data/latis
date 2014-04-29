package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory

/**
 * Keep only the first sample of any outer Function in the Dataset.
 */
class FirstFilter extends Filter {
  //TODO: could we return the Sample not nested within the Function (i.e. reduced)?
  override def applyToFunction(function: Function) = Some(Function(Seq(function.iterator.next)))
}

object FirstFilter extends OperationFactory {
  override def apply(): FirstFilter = new FirstFilter
}
