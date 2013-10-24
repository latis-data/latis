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
protected class Selection(val vname: String, val operation: String, val value: String) extends Operation {
  //TODO: consider abstracting out common "filter" stuff
  //TODO: if domain, delegate to DomainSet
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = variable match {
    case text: Text => filterText(text)
    case s: Scalar => filterScalar(s)
    case t: Tuple => filterTuple(t)
    case f: Function => filterFunction(f)
  }
    
  def filterText(text: Text): Option[Text] = {
    if (vname == text.getName) operation match {
      case "=~" => {
        if (text.value.matches(value)) Some(text) 
        else None //regex
      }
      case _    => if (isValid(text.compare(value))) Some(text) else None //like any other scalar
    } else Some(text) //operation doesn't apply to this Scalar Variable, no-op
  }
  
  def filterScalar(scalar: Scalar): Option[Scalar] = {
    if (vname == scalar.getName) {
      if (isValid(scalar.compare(value))) Some(scalar) else None
    } else Some(scalar) //operation doesn't apply to this Scalar Variable, no-op
  }
  
  /**
   * Return None (exclude) if any member Variable does not pass the selection.
   */
  def filterTuple(tuple: Tuple): Option[Tuple] = {
    //TODO: does this short circuit?
    //are any member variables invalid (None) after filtering
    tuple.getVariables.map(filter(_)).find(_.isEmpty) match {
      case Some(_) => None //found an invalid variable, exclude the entire tuple
      case None => Some(tuple)
    }
  }
  
  def filterFunction(f: Function): Option[Function] = Some(FilteredFunction(f, this))

//  def filterSample(sample: Sample): Option[Sample] = {
//    for (d <- filter(sample.domain); r <- filter(sample.range)) yield Sample(d,r)
//  }
  
  //TODO: support NOT (!)
  
  private def isValid(comparison: Int): Boolean = {
    (comparison < 0 && operation.contains("<")) || 
    (comparison > 0 && operation.contains(">")) || 
    (comparison == 0 && operation.contains("="))
  }
  
  override def toString = vname + operation + value
}


object Selection {
  
  def apply(vname: String, operation: String, value: String) = {
    //Deprecate use of "=" for selections. Replace with "==".? TODO: maybe not
    val op = operation match {
      case "=" => "=="
      case _ => operation
    }
    
    new Selection(vname, op, value)
  }
  
  def apply(expression: String): Selection = expression match {
    case SELECTION(name, op, value) => Selection(name, op, value)
    //TODO: case _ => error
  }
  
  //Extract the selection as a single string expression
  def unapply(sel: Selection): Option[String] = Some(sel.toString)
}