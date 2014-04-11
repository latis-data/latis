package latis.ops

import latis.dm._
import latis.time._
import latis.util.RegEx._
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Filter based on (in)equality.
 * e.g. "foo >= 2"
 * double? text?
 * Data value class?
 * 
 */
protected class Selection(val vname: String, val operation: String, val value: String) extends Operation with Logging {
  //TODO: consider abstracting out common "filter" stuff
  //TODO: if domain, delegate to DomainSet
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = {
    //If the filtering causes an exception, log a warning and return None.
    try {
      variable match {
      //Special handling for Text regex matching with "=~"
      // except Time which can be handled like any other Scalar.
      case text: Text if (! text.isInstanceOf[Time]) => filterText(text)
      case s: Scalar   => filterScalar(s)
      case t: Tuple    => filterTuple(t)
      case f: Function => filterFunction(f)
      }
    } catch {
      case e: Exception => {
        logger.warn("Selection filter threw an exception: " + e.getMessage, e)
        None
      }
    }
  }
    
  def filterText(text: Text): Option[Text] = {
    if (text.hasName(vname)) operation match {
      case "=~" => {
        if (text.getValue.asInstanceOf[String].matches(value)) Some(text) //TODO: getStringValue on Variable?
        else None //regex
      }
      case _ => if (isValid(text.compare(value))) Some(text) else None //like any other scalar
    } else Some(text) //operation doesn't apply to this Scalar Variable, no-op
  }
  
  def filterScalar(scalar: Scalar): Option[Scalar] = {
    if (scalar.hasName(vname)) {
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

  
  private def isValid(comparison: Int): Boolean = {
    if (operation == "!=") {
      comparison != 0
    } else {
      (comparison < 0  && operation.contains("<")) || 
      (comparison > 0  && operation.contains(">")) || 
      (comparison == 0 && operation.contains("="))
    }
  }
  
  override def toString = vname + operation + value
}


object Selection {
  
  def apply(vname: String, operation: String, value: String) = new Selection(vname, operation, value)
  
  def apply(expression: String): Selection = expression.trim match {
    case SELECTION.r(name, op, value) => Selection(name, op, value)
    case _ => throw new Error("Failed to make a Selection from the expression: " + expression)
  }
  
  //Extract the selection as a triple
  def unapply(sel: Selection): Option[(String, String, String)] = Some((sel.vname, sel.operation, sel.value))
}