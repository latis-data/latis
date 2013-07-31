package latis.ops.filter

import latis.dm._
import scala.Option.option2Iterable
import latis.ops.Operation

/**
 * Filter out samples of outer Function only?
 * filter vars from tuple?
 * Scalars: ?
 *   replace with NaN, ""?
 *   or drop entire sample, like now
 *   use a "replace" operation for other cases?
 * 
 * Think of Dataset as a collection of Variables
 * like scala collections, with filter (and map for morphing operations)
 *   dataset.filter(f: V => Boolean)
 *   but need to recurse on Tuples and Functions
 * focus on Tuple as collection of V and Function as collection of samples
 *   each can be treated as a diff monad/collection
 *   Dataset is a collection of those, monad of monads? (monad transformer?)
 *   
 * ProjectionFilter for excluding vars in a tuple
 * SelectionFilter for excluding samples in a Function
 * 
 * dataset.filter(expr = "time > 2") => SelectionFilter(expr).filter(dataset)
 * 
 */
abstract class Filter extends Operation {

  def apply(dataset: Dataset): Dataset = {
    Dataset(dataset.variables.flatMap(filter(_)))
    //TODO: provenance metadata
  }
  
  def filter(variable: Variable): Option[Variable] = variable match {
    case s: Scalar => filterScalar(s)
    case t: Tuple => filterTuple(t)
    //case Tuple(vars) => Some(Tuple(vars.flatMap(filter(_))))
    //TODO: flatten if tuple has no name, no need to preserve namespace
    
//    case f: Function => filterFunction(f)
  }
  
  //filter domain and range components, exclude if either is excluded
  def filterSample(sample: Sample): Option[Sample] = {
    for (d <- filter(sample.domain); r <- filter(sample.range)) yield Sample(d,r)
  }
  
  def filterScalar(scalar: Scalar): Option[Scalar] = Some(scalar) //no-op

  def filterTuple(tuple: Tuple): Option[Tuple] = {
    val vs = tuple.variables.flatMap(filter(_))
    val tup = Tuple(vs)
    Some(tup)
  }
  
  //wrap Function, apply filter to each sample while iterating
  def filterFunction(f: Function): Option[Function] = ??? //Some(FilteredFunction(f, this))
  
}

