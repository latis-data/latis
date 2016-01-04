package latis.ops.filter

import latis.dm.Function
import latis.dm.Scalar
import latis.dm.Text
import latis.time.Time
import latis.util.RegEx.SELECTION
import com.typesafe.scalalogging.LazyLogging
import latis.util.iterator.MappingIterator
import latis.dm.Sample
import latis.dm.Variable
import latis.dm.WrappedFunction
import latis.dm.Tuple
import latis.ops.Operation
import latis.ops.resample.NearestNeighbor
import latis.util.StringUtils
import latis.ops.OperationFactory
import latis.dm.Dataset

/**
 * Filter based on a basic boolean expression.
 * e.g. "foo >= 2"
 */
class Selection(val vname: String, val operation: String, val value: String) extends Filter with LazyLogging {
  //TODO: if domain, delegate to DomainSet

  override def apply(ds: Dataset) = ds.findVariableByName(vname) match {
    case None => throw new UnsupportedOperationException(s"Cannot select on unknown variable '$vname'")
    case _ => super.apply(ds)
  }
  
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
  
  /**
   * If we are selecting on the Function domain and we have bounds data 
   * in the range and the operation include ">" or "<", apply a new 
   * Selection using the bounds.
   */
  override def applyToFunction(f: Function): Option[Function] = {
   (f.getDomain.hasName(vname), f.getRange.findVariableByName("bounds")) match {
      case (true, Some(Tuple(vars))) => operation match {
        case op if(op.contains("<")) => Selection(vars(0).getName, operation, value).applyToFunction(f).asInstanceOf[Option[Function]]
        case op if(op.contains(">")) => Selection(vars(1).getName, operation, value).applyToFunction(f).asInstanceOf[Option[Function]]
      }
      case _ => super.applyToFunction(f)
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


object Selection extends OperationFactory {
  
  override def apply(args: Seq[String]): Operation = {
    //should be only one arg: expression
    Selection(args.head)
  }
  
  def apply(vname: String, operation: String, value: String): Operation = {
    //delegate to NearestNeighbor filter for '~' operator
    if (operation == "~") NearestNeighborFilter(vname, value)
    
    //validate time selections before they are applied to every Sample
    else vname match {
      case "time" => { 
        if(Time.isValidIso(value) || StringUtils.isNumeric(value)) new Selection(vname, operation, value)
        else throw new UnsupportedOperationException(
          s"Invalid Selection: could not parse '$value' as a time string.")
      }
      case _ => new Selection(vname, operation, value)
    }
  }
  
  def apply(expression: String): Operation = expression.trim match {
    case SELECTION.r(name, op, value) => Selection(name, op, value)
    case _ => throw new Error("Failed to make a Selection from the expression: " + expression)
  }
  
  //Extract the selection as a triple
  def unapply(sel: Selection): Option[(String, String, String)] = Some((sel.vname, sel.operation, sel.value))
}