package latis.ops.filter

import latis.dm.Scalar
import latis.dm.Text
import latis.time.Time
import latis.util.RegEx.SELECTION

import com.typesafe.scalalogging.slf4j.Logging

/**
 * Filter based on a basic boolean expression.
 * e.g. "foo >= 2"
 */
protected class Selection(val vname: String, val operation: String, val value: String) extends Filter with Logging {
  //TODO: if domain, delegate to DomainSet

  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    //If the filtering causes an exception, log a warning and return None.
    try {
      scalar match {
      //Special handling for Text regex matching with "=~"
      // except Time which can be handled like any other Scalar.
      case text: Text if (! text.isInstanceOf[Time]) => applyToText(text)
      case s: Scalar => if (scalar.hasName(vname)) {
        if (isValid(scalar.compare(value))) Some(scalar) else None
        } else Some(scalar) //operation doesn't apply to this Scalar Variable, no-op
      }
    } catch {
      case e: Exception => {
        logger.warn("Selection filter threw an exception: " + e.getMessage, e)
        None
      }
    }
  }
    
  def applyToText(text: Text): Option[Text] = {
    if (text.hasName(vname)) operation match {
      case "=~" => {
        if (text.getValue.asInstanceOf[String].matches(value)) Some(text) //TODO: getStringValue on Variable?
        else None //regex
      }
      case _ => if (isValid(text.compare(value))) Some(text) else None //like any other scalar
    } else Some(text) //operation doesn't apply to this Scalar Variable, no-op
  }
  
  
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