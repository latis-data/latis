package latis.ops.filter

import latis.dm.Function
import latis.dm.Scalar
import latis.dm.Text
import latis.time.Time
import latis.util.RegEx.SELECTION
import com.typesafe.scalalogging.slf4j.Logging
import latis.util.iterator.MappingIterator
import latis.dm.Sample
import latis.dm.Variable
import latis.dm.WrappedFunction
import latis.dm.Tuple
import latis.ops.Operation
import latis.ops.resample.NearestNeighbor

/**
 * Filter based on a basic boolean expression.
 * e.g. "foo >= 2"
 */
protected class Selection(val vname: String, val operation: String, val value: String) extends Filter with Logging {
  //TODO: if domain, delegate to DomainSet
  //TODO: change operation to operator?
  
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
        logger.warn("Selection filter threw an exception: " + e.getMessage)
        None
      }
    }
  }
    
  def applyToText(text: Text): Option[Text] = {
    if (text.hasName(vname)) operation match {
      case "=~" => text match {
        case Text(text_value) if (text_value.matches(value)) => Some(text) //keep match
        case _ => None
      }
      case "!=~" => text match {
        case Text(text_value) if (text_value.matches(value)) => None //exclude match
        case _ => Some(text)
      }
      case _ => {
        val cmp = text.compare(value)
        if (isValid(cmp)) Some(text) else None //like any other scalar
      }
    } else Some(text) //operation doesn't apply to this Scalar Variable, no-op
  }
  
  override def applyToSample(sample: Sample): Option[Sample] = {
    val x = sample.getVariables.map(applyToVariable(_))
    x.find(_.isEmpty) match{
      case Some(_) => None //found an invalid variable, exclude the entire sample
      case None => Some(Sample(x(0).get, x(1).get))
    }
  }
  
  override def applyToTuple(tuple: Tuple): Option[Tuple] = {
    val x = tuple.getVariables.map(applyToVariable(_))
    x.find(_.isEmpty) match{
      case Some(_) => None //found an invalid variable, exclude the entire tuple
      case None => Some(Tuple(x.map(_.get), tuple.getMetadata))
    }
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
  
  def apply(vname: String, operation: String, value: String): Operation = {
    //delegate to NearestNeighbor filter for '~' operator
    if (operation == "~") NearestNeighborFilter(vname, value)
    else new Selection(vname, operation, value)
  }
  
  def apply(expression: String): Operation = expression.trim match {
    case SELECTION.r(name, op, value) => Selection(name, op, value)
    case _ => throw new Error("Failed to make a Selection from the expression: " + expression)
  }
  
  //Extract the selection as a triple
  def unapply(sel: Selection): Option[(String, String, String)] = Some((sel.vname, sel.operation, sel.value))
}