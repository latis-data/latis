package latis.ops

import scala.collection.mutable.ArrayBuffer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.WrappedFunction

/**
 * Reduce any Tuples of one element to that element and reduce any 
 * Functions with one Sample to that Sample.
 */
class Reduction extends Operation  {
  
  /**
   * Apply to the domain and range of the sample and package in a new sample.
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(d,r) => for (d2 <- applyToVariable(d); r2 <- applyToVariable(r)) yield Sample(d2,r2)
  }
  
  /**
   * Recursively apply to all elements.
   * If a Tuple has only one element, reduce it to that element.
   */
  override def applyToTuple(tuple: Tuple): Option[Variable] = {
    //TODO: assumes the tuple does not own the data
    //TODO: don't reduce if tuple is named, preserve namespace
    //  option to force?
    //  what about other metadata? concat names with "_"?
    val vars = tuple.getVariables.flatMap(applyToVariable(_)) 
    vars.length match {
      case 0 => None
      case 1 => Some(vars.head) //drop the redundant Tuple layer
      case _ => {
        //flatten, e.g. (a,(b,c)) => (a,b,c)
        //Since we are a Tuple, we can contain the elements of nested Tuples (i.e. flatten)
        //TODO: will this recurse
        val flattenedVars = ArrayBuffer[Variable]()
        for (v <- vars) v match {
          case Tuple(vs) => flattenedVars ++= vs //flatten nested Tuple
          case _ => flattenedVars += v
        }
        Some(Tuple(flattenedVars)) //TODO: metadata
      }
    }
  }

  /**
   * If the given Function has only one sample, reduce to that Sample.
   * If the domain of that sample is an Index, just keep the range.
   * If the Function has no samples, return None.
   */
  override def applyToFunction(function: Function) = {
    //TODO: TraversableOnce issues?
    val n = function.getLength
    if (n == 0) None
    else if (n == 1) applyToSample(function.iterator.next) match {
      case Some(Sample(_: Index, range)) => Some(range)
      case ov: Option[Variable] => ov
    }
    else Some(WrappedFunction(function, this))
  }
  
}

object Reduction extends OperationFactory {

  override def apply() = new Reduction()
  
  def reduce(dataset: Dataset): Dataset = {
    (new Reduction)(dataset)
  }
}