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
//TODO: Organize imports
/**
 * Filter based on a constraint expression of the form
 * "a = {1,2,3}"
 */
class Contains(val vname: String, val values: Seq[String]) extends Filter with LazyLogging {

  override def apply(ds: Dataset): Dataset = ds match {
    case Dataset(v) => ds.findVariableByName(vname) match {
      case None => throw new UnsupportedOperationException(s"Dataset does not contain unknown variable '$vname'")
      case _ => super.apply(ds)
    }
    case _ => ds
  }
  
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    //If the filtering causes an exception, log a warning and return None.
    try {
      scalar match {
      case s: Scalar => if (scalar.hasName(vname)) {
        if (values.exists(v => isValid(scalar.compare(v)))) Some(scalar) else None //TODO: this apparently throws an exception when 'scalar' is an int or real and 'v' is a string 
        } else Some(scalar) //operation doesn't apply to this Scalar Variable, no-op
      }
    } catch {
      case e: Exception => {
        logger.warn("Contains filter threw an exception: " + e.getMessage) 
        None
      }
    }
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
  
  private def isValid(comparison: Int): Boolean = comparison == 0 //TODO: make sure this is facilitating scalar==scalar(value) correctly
  
  override def toString: String = { 
    var str = s"$vname={" 
    for (v <- values) {
      str += s"$v,"
    }
    str.dropRight(1) + "}"
  }
  
}


object Contains extends OperationFactory {
  
  override def apply(args: Seq[String]): Operation = {
//    //should be only one arg: expression
//    Selection(args.head)
    ???
  }
  
  def apply(vname: String, value: String, values: String*): Operation = {
//    //delegate to NearestNeighbor filter for '~' operator
//    if (operation == "~") { NearestNeighborFilter(vname, value) }
//    
//    //validate time selections before they are applied to every Sample
//    else {
//      vname match {
//        case "time" => {
//          if (Time.isValidIso(value) || StringUtils.isNumeric(value)) {
//            new Selection(vname, operation, value)
//          }
//          else {
//            throw new UnsupportedOperationException(
//              s"Invalid Selection: could not parse '$value' as a time string.")
//          }
//        }
//        case _ => new Selection(vname, operation, value)
//      }
//    }
    ???
  }
  
  def apply(expression: String): Operation = /*expression.trim match*/ {
//    case SELECTION.r(name, op, value) => Selection(name, op, value)
//    case _ => throw new Error("Failed to make a Selection from the expression: " + expression)
    ???
  }
  
  //Extract the selection as a triple
  def unapply(sel: Selection): Option[(String, String, String)] = ???//Some((sel.vname, sel.operation, sel.value))
}