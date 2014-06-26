package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample

/**
 * Keep only the last sample of any outer Function in the Dataset.
 */
class LastFilter extends Filter {

  override def applyToFunction(function: Function) = {
    //TODO: IterableOnce problem?
    //val s = function.iterator.drop(function.getLength-1).next
    val l = function.iterator.toList
    l.length match{
      case 0 => Some(Sample(function.getDomain, function.getRange))
      case _ => Some(l.last)
    }
  }
}

object LastFilter extends OperationFactory {
  override def apply() = new LastFilter
}