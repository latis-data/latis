package latis.ops

import latis.dm._
import scala.Option.option2Iterable

/**
 * Exclude variables not named in the given list.
 */
class Projection(val names: Seq[String]) extends Operation {
  //TODO: support long names  
  //TODO: support aliases, see Variable.hasAlias
  //TODO: preserve order of requested variables
  //TODO: what if only part of nD domain is selected? only if it is a product set
  //TODO: return domain as IndexFunction if only it is projected?
  //TODO: sanitize: make sure no special chars...
  
  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.variables.flatMap(project(_)))
    //TODO: provenance metadata
  }
  
  def project(variable: Variable): Option[Variable] = variable match {
    case i: Index => Some(i) //always project Index, often used as a place holder for a domain
    case s: Scalar   => projectScalar(s)
    case t: Tuple    => projectTuple(t)
    case f: Function => projectFunction(f)
  }
   
  def projectScalar(scalar: Scalar): Option[Scalar] = {
    //TODO: support alias
    if (names contains scalar.name) Some(scalar) else None
  }
  
  def projectTuple(tuple: Tuple): Option[Tuple] = {
    /*
     * TODO: maintain projection order
     * making a new dataset, tuple reuses same kids
     * need to deal with Data!
     * otherwise will only work for column-oriented data (all in scalars, GranuleAdapter)
     * Is this safe to do here or support in adapters?
     * Apply as a FilteredFunction so we can reorder vars in sample?
     */
    val vars = tuple.variables.flatMap(project(_))
    if (vars.length == 0) None
    else Some(Tuple(vars)) //TODO: metadata
  }
    
  def projectFunction(function: Function): Option[Function] = {
    //if function does not have the data, delegate to the kids, saves having to parse data 
    if (function.data.isEmpty) { //delegate to kids
      //only an optimization for column oriented data?
/*
 * TODO: 
 * 1) if domain not projected, replace with Index
 * 2) if range not projected, make domain the range of a function of index
 * 
 * wrapper is problematic because projectSample doesn't know index
 *   pass in index from iterator?
 *   projectSample is only used by ProjectedFunction, so we can take some liberties
 * NullVariable?
 *   or some other indicator that the iterator can interpret and replace
 * 
 */
      for (d <- project(function.domain); r <- project(function.range)) yield Function(d,r) //TODO: metadata
    } else { //wrap function
      Some(ProjectedFunction(function, this))
    }
  }
      
  def projectSample(sample: Sample): Option[Sample] = {
//TODO: if domain not projected, replace with Index
//    val domain = project(sample.domain) match {
//      case Some(v) => v
//      case None => ??? //TODO: we don't know what sample (index) we are on!
    //but at this point we shouldn't need the index value?
//    }
    for (d <- project(sample.domain); r <- project(sample.range)) yield Sample(d,r) //TODO: metadata
  }
  
  
  override def toString = names.mkString(",")
  
}

object Projection {
  
  def apply(names: Seq[String]) = new Projection(names)
  def apply(expression: String) = new Projection(expression.split(","))
  
  def unapply(proj: Projection) = Some(proj.toString)
}