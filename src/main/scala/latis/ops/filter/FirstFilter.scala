package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample

/**
 * Keep only the first sample of any outer Function in the Dataset.
 */
class FirstFilter extends Filter {
  override def applyToFunction(function: Function) = {
    val l = function.iterator.toList
    l.length match {
      case 0 => Some(Sample(function.getDomain, function.getRange))
      case _ => Some(l.head)
    }
  }
}

object FirstFilter extends OperationFactory {
  override def apply(): FirstFilter = new FirstFilter
}
