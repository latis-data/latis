package latis.ops

import latis.dm.Dataset
import latis.dm.Sample
import latis.dm.Tuple
import latis.dm.Variable

/**
 * Reduce any Tuples of one element to that element.
 */
class ReduceTuple extends Operation2  {
  
  /**
   * Recursively apply to all elements.
   * If a Tuple has only one element, reduce it to that element.
   */
  override def applyToTuple(tuple: Tuple): Option[Variable] = tuple match {
    //TODO: assumes the tuple does not own the data
    //TODO: preserve namespace with dot (.) notation
    case Tuple(Seq(v: Variable)) => Some(v)
    case Tuple(Seq()) => None
    case _ => Some(tuple)
  }
  
}

object ReduceTuple {

  def apply() = new ReduceTuple()
  
  def reduce(dataset: Dataset): Dataset = {
    (new ReduceTuple)(dataset)
  }
}