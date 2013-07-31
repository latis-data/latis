package latis.ops.filter

import latis.dm._
import latis.util.RegEx._

/**
 * Filter based on (in)equality.
 * e.g. "foo >= 2"
 * double? text?
 * Data value class?
 * 
 */
class SelectionFilter(vname: String, op: String, value: Double) extends Filter {
  
  //TODO: if domain, delegate to DomainSet
  
  override def filterScalar(scalar: Scalar): Option[Scalar] = {
    
    if (vname == scalar.name) {
      scalar match {
        case Number(d) =>  {
          if (isValid(d.compare(value))) Some(scalar) else None
        }
        //TODO: Text, also regex?
      }
    } else Some(scalar) //doesn't apply to this Scalar Variable, no-op
  }
  
  /**
   * Return None (exclude) if any member Variable does not pass the selection.
   */
  override def filterTuple(tuple: Tuple): Option[Tuple] = {
    //TODO: does this short circuit?
    //are any member variables invalid (None) after filtering
    tuple.variables.map(filter(_)).find(_.isEmpty) match {
      case Some(_) => None //found an invalid variable, exclude the entire tuple
      case None => Some(tuple)
    }
  }
  
  private def isValid(comparison: Int): Boolean = {
    (comparison < 0 && op.contains("<")) || 
    (comparison > 0 && op.contains(">")) || 
    (comparison == 0 && op.contains("="))
  }
}

object SelectionFilter {
  
  def apply(vname: String, op: String, value: String) = new SelectionFilter(vname, op, value.toDouble)
  //TODO: support text values
  
  def apply(expression: String): SelectionFilter = expression match {
    case SELECTION(name, op, value) => SelectionFilter(name, op, value)
    //TODO: case _ => error
  }
}