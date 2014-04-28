package latis.ops

import latis.dm._
//import scala.Option.option2Iterable
import latis.util.RegEx._

/**
 * Exclude variables not named in the given list.
 */
class Projection(val names: Seq[String]) extends SampleMappingOperation {
  //TODO: support long names, e.g. tupA.foo  , build into hasName?
  //TODO: preserve order of requested variables
  //TODO: consider projecting only part of nD domain. only if it is a product set

  /**
   * WrappedFunction should use applyToSample only to get domain and range types for the sampleTemplate
   * If the domain is Index, then it will only apply op to the range.
   * Thus we don't need to insert index values here.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    val pd = applyToVariable(sample.domain)
    val pr = applyToVariable(sample.range)
    (pd,pr) match {
      case (Some(d), Some(r)) => Some(Sample(d,r))
      case (None, Some(r))    => Some(Sample(Index(), r)) //TODO: do we need a valid value here? 
      case (Some(d), None)    => Some(Sample(Index(), d)) //no range, so make domain the range of an index function
      case (None, None) => ??? //TODO: nothing projected, could return Null but is it an error if we get this far?
    }
  }
  
  override def applyToTuple(tuple: Tuple): Option[Tuple] = {
    val vars = tuple.getVariables.flatMap(applyToVariable(_))
    if (vars.length == 0) None
    else Some(Tuple(vars)) //TODO: metadata
  }
  
  //override def applyToFunction(function: Function): Option[Variable] = Some(ProjectedFunction(function, this))
  
  override def applyToVariable(variable: Variable): Option[Variable] = {
    //Note, always project Index since it is used as a place holder for a non-projected domain.
    if (variable.isInstanceOf[Index] || names.exists(variable.hasName(_))) Some(variable) 
    else super.applyToVariable(variable)
  }
   
  override def applyToScalar(scalar: Scalar): Option[Scalar] = super.applyToScalar(scalar) match {
    case Some(i: Index) => Some(i) //always project Index since it is used as a place holder for a non-projected domain.
    case _ => None //name didn't match above
  }
  
//  def projectScalar(scalar: Scalar): Option[Scalar] = {
//    if (names.exists(scalar.hasName(_))) Some(scalar) else None
//    //TODO: do name test for all
//  }
//  
//  def projectTuple(tuple: Tuple): Option[Tuple] = {
//    /*
//     * TODO: maintain projection order
//     * making a new dataset, tuple reuses same kids
//     * need to deal with Data!
//     * otherwise will only work for column-oriented data (all in scalars, GranuleAdapter)
//     * Is this safe to do here or support in adapters?
//     * Apply as a FilteredFunction so we can reorder vars in sample?
//     * 
//     * only becomes a problem if tuples are allowed to contain data
//     * what about nested functions...?
//     * note, Sample is managed in ProjectedFunction
//     */
//    val vars = tuple.getVariables.flatMap(projectVariable(_))
//    if (vars.length == 0) None
//    else Some(Tuple(vars)) //TODO: metadata
//  }
//    
//  //Let ProjectedFunction deal with this.
//  def projectFunction(function: Function): Option[Function] = Some(ProjectedFunction(function, this))
//  //TODO: consider projected function by name (instead of its scalars), similar to long name support

  override def toString = names.mkString(",")
}

object Projection {
  
  def apply(names: Seq[String]) = new Projection(names)
    
  def apply(expression: String): Projection = expression.trim match {
    //case PROJECTION.r(names) => //will only expose first and last
    case s: String if (s matches PROJECTION) => Projection(s.split(""",\s*""")) //Note, same delimiter used in PROJECTION def
    case _ => throw new Error("Failed to make a Projection from the expression: " + expression)
  }
  
  //Extract the list of projected variable names
  def unapply(proj: Projection) = Some(proj.names)

}
