package latis.ops

import scala.Option.option2Iterable

import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.util.RegEx.PROJECTION
import latis.util.iterator.MappingIterator

/**
 * Exclude variables not named in the given list.
 */
class Projection(val names: Seq[String]) extends Operation with Idempotence {
  //TODO: support long names, e.g. tupA.foo  , build into hasName?
  //TODO: consider projecting only part of nD domain. only if it is a product set
  
  /**
   * Test if the given Variable matches a name in the projection.
   * If not, follow the usual recursion.
   */
  override def applyToVariable(variable: Variable): Option[Variable] = {
    if (names.exists(variable.hasName(_))) Some(variable) 
    else super.applyToVariable(variable)
  }
   
  /**
   * Should only get here if the scalar name didn't match. Make sure we let Index pass.
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = scalar match {
    case i: Index => Some(i) //always project Index since it is used as a place holder for a non-projected domain.
    case _ => None //name didn't match above
  }
  
  /**
   * Used to process each sample.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    val pd = applyToVariable(sample.domain)
    val pr = applyToVariable(sample.range)
    (pd,pr) match {
      case (Some(d), Some(r))     => Some(Sample(d,r))
      case (None, Some(r))        => Some(Sample(Index(), r)) //no domain, so replace it with Index
      case (Some(_: Index), None) => None //Index is just a place holder, no need to keep "index -> index" Function
      case (Some(d), None)        => Some(Sample(Index(), d)) //no range, so make it an index Function of the domain
      case (None, None)           => None //Note, could be an inner Function that is not projected
    }
  }
  
  /**
   * Apply to all elements in the Tuple. 
   * Return None if all elements are excluded.
   * If only one element in the Tuple is projected, return that element.
   */
  override def applyToTuple(tuple: Tuple): Option[Variable] = {
    //val vars = names.flatMap(tuple.findVariableByName(_)).flatMap(applyToVariable(_)) //projection order
    val vars = tuple.getVariables.flatMap(applyToVariable(_)) //orig var order
    vars.length match {
      case 0 => None
      case 1 => {
        //reduce a Tuple of one element to that element
        //TODO: if the Tuple had a name, append it to the var name with a "."
        val v = vars.head 
//        val md = tuple.getMetadata("name") match {
//          case Some(s) => v.getMetadata() + ("name" -> s + "." + v.getName)
//          case None => v.getMetadata()
//        }
//        Some(v.copy(md))
        Some(v)
      }
      case _ => Some(Tuple(vars, tuple.getMetadata)) //assumes Tuple does not contain data
    }
  }
  
  /**
   * Override to eliminate special nesting logic.
   */
  override def applyToFunction(function: Function): Option[Variable] = {
    val mit = new MappingIterator(function.iterator, (s: Sample) => this.applyToSample(s))
    val template = applyToSample(function.getSample) match {
      case None => return None //no matches in function
      case Some(s) => s
    }
    Some(Function(template.domain, template.range, mit, function.getMetadata))
  }


  override def toString = names.mkString(",")
}

object Projection extends OperationFactory {
  
  override def apply(names: Seq[String]) = new Projection(names)
    
  def apply(expression: String): Projection = expression.trim match {
    //case PROJECTION.r(names) => //Note, regex match will only expose first and last
    case s: String if (s matches PROJECTION) => Projection(s.split(""",\s*""")) //Note, same delimiter used in PROJECTION def
    case _ => throw new Error("Failed to make a Projection from the expression: " + expression)
  }
  
  //Extract the list of projected variable names
  def unapply(proj: Projection) = Some(proj.names)

}
