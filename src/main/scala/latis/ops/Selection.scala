package latis.ops

import latis.dm._
import latis.util.RegEx._

/**
 * Filter based on (in)equality.
 * e.g. "foo >= 2"
 * double? text?
 * Data value class?
 * 
 */
protected class Selection(val vname: String, val op: String, val value: String) extends Operation {
  //TODO: consider abstracting out common "filter" stuff
  //TODO: if domain, delegate to DomainSet
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.variables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = variable match {
    case s: Scalar => filterScalar(s)
    case t: Tuple => filterTuple(t)
    case f: Function => filterFunction(f)
  }
    
  def filterScalar(scalar: Scalar): Option[Scalar] = {
    if (vname == scalar.name) {
      if (isValid(scalar.compare(value))) Some(scalar) else None
      
//      scalar match {
//        case Number(d) =>  {
//          if (isValid(d.compare(value.toDouble))) Some(scalar) else None //TODO: cache Double value?
//          //TODO: formatted time to double
//        }
//        //TODO: Text, also regex?
//      }
    } else Some(scalar) //doesn't apply to this Scalar Variable, no-op
  }
  
  /**
   * Return None (exclude) if any member Variable does not pass the selection.
   */
  def filterTuple(tuple: Tuple): Option[Tuple] = {
    //TODO: does this short circuit?
    //are any member variables invalid (None) after filtering
    tuple.variables.map(filter(_)).find(_.isEmpty) match {
      case Some(_) => None //found an invalid variable, exclude the entire tuple
      case None => Some(tuple)
    }
  }
  
  def filterFunction(f: Function): Option[Function] = Some(FilteredFunction(f, this))

  def filterSample(sample: Sample): Option[Sample] = {
    for (d <- filter(sample.domain); r <- filter(sample.range)) yield Sample(d,r)
  }
  
  
  private def isValid(comparison: Int): Boolean = {
    (comparison < 0 && op.contains("<")) || 
    (comparison > 0 && op.contains(">")) || 
    (comparison == 0 && op.contains("="))
  }
  
  override def toString = vname + op + value
}


object Selection {
  
  def apply(vname: String, op: String, value: String) = {
    new Selection(vname, op, value)
  }
  
  def apply(expression: String): Selection = expression match {
    case SELECTION(name, op, value) => Selection(name, op, value)
    //TODO: case _ => error
  }
  
  //Extract the selection as a single string expression
  def unapply(sel: Selection): Option[String] = Some(sel.toString)
}