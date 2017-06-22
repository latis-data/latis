package latis.ops

import latis.dm.Function
import latis.dm.Variable

class Sort extends Operation {
  override def applyToFunction(function: Function): Option[Function] = {

    // get samples from function and sort them
    val samples: Seq[Variable] = function.iterator.toSeq
    val sortedSamples = samples.sorted

    // update length metadata
    val md = function.getMetadata + ("length" -> sortedSamples.length.toString)

    //make the new function
    sortedSamples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(sortedSamples))
    }
  }
}

object Sort extends OperationFactory {
  override def apply: Sort = new Sort
}