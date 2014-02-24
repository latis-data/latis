package latis.ops

import latis.dm._
import scala.Option.option2Iterable

/**
 * Exclude variables not named in the given list.
 */
class Projection(val names: Seq[String]) extends Operation {
  //TODO: support long names, e.g. tupA.foo  
  //TODO: preserve order of requested variables
  //TODO: consider projecting only part of nD domain. only if it is a product set
  //TODO: sanitize: make sure no special chars..., do via pattern match sufficient?
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.getVariables.flatMap(project(_)))
    //TODO: provenance metadata
  }
  
  def project(variable: Variable): Option[Variable] = variable match {
    case i: Index => Some(i) //always project Index, often used as a place holder for a domain
    case s: Scalar   => projectScalar(s)
    case t: Tuple    => projectTuple(t)
    case f: Function => projectFunction(f)
  }
   
  def projectScalar(scalar: Scalar): Option[Scalar] = {
    if (names.exists(scalar.hasName(_))) Some(scalar) else None
  }
  
  def projectTuple(tuple: Tuple): Option[Tuple] = {
    /*
     * TODO: maintain projection order
     * making a new dataset, tuple reuses same kids
     * need to deal with Data!
     * otherwise will only work for column-oriented data (all in scalars, GranuleAdapter)
     * Is this safe to do here or support in adapters?
     * Apply as a FilteredFunction so we can reorder vars in sample?
     * 
     * only becomes a problem if tuples are allowed to contain data
     * what about nested functions...?
     * note, Sample is managed in ProjectedFunction
     */
    val vars = tuple.getVariables.flatMap(project(_))
    if (vars.length == 0) None
    else Some(Tuple(vars)) //TODO: metadata
  }
    
  //Let ProjectedFunction deal with this.
  def projectFunction(function: Function): Option[Function] = Some(ProjectedFunction(function, this))
  //TODO: consider projected function by name (instead of its scalars), similar to long name support

  override def toString = names.mkString(",") //TODO: inside "Projection()"
}

object Projection {
  
  def apply(names: Seq[String]) = new Projection(names)
  def apply(expression: String) = new Projection(expression.replaceAll("""\s""","").split(","))
  
  def unapply(proj: Projection) = Some(proj.toString)
}