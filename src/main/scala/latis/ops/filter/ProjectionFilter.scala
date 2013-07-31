package latis.ops.filter

import latis.dm._
import scala.Option.option2Iterable

class ProjectionFilter(names: Seq[String]) extends Filter {
  //exclude vars not named in the given list
   
  override def filter(variable: Variable): Option[Variable] = {
    if (names contains variable.name) Some(variable) //could match Tuple or Function as well as Scalar
    else super.filter(variable) //Variable wasn't specifically matched, so recurse
  }
  
  //no-op since we've already tried to match it by name
 //? override def filterTuple(tuple: Tuple): Option[Tuple] = Some(tuple)
  /*
   * TODO: need to do more than support filter when iterating
   * can't simply wrap it
   * need to munge model
   * e.g. exclude vars in tuple range
   * 
   */
  override def filterFunction(f: Function): Option[Function] = {
//TODO: test with data from kids or from Function
    //  may need to get clever with indexing if Function has the data and we drop an interleved variable
    // do we need to handle both cases here? use FilteredFunction if f has data?

    /*
     * TODO: 1013-07-15 
     *  FilteredFunction should work but need to modify model
     *   override domain, range in FilteredFunction?
     *   or filter as below in factory?
     *   does this apply to selection also?
     *     we don't want to trigger data reading, e.g. iteration of inner Function
     *     just wrap the inner function, too
     *     ++but selection filter on scalar will want data
     *     selection has no potential for changing model, only a projection issue?
     *     build FF here
     *   overriding range and domain seems best
     *     as opposed to modifying state, filter as requested
     *     FF extends F, would be nice if we could pass filtered domain and range to super constructor
     *     even worse for TransformedFunction, 
     *   ProjectedFunction?
     *     probably, FF is not allowed to change type
     * 
     * ++ can projection be treated as a filter? it changes the model!
     *   but only reduces it
     *   can think of it as nulling out some data?
     *   still quite different than dropping function samples
     * 
     * consider usage with filter, map, fold...
     * 
     * 
     */
    
    //for (d <- filter(f.domain); r <- filter(f.range)) yield FilteredFunction(d,r)
    
//    if (f.data.notEmpty) Some(FilteredFunction(f, this))
 //   else for (d <- filter(f.domain); r <- filter(f.range)) yield Function(d,r)
    ???
  }
  
  override def filterScalar(scalar: Scalar): Option[Scalar] = {
    if (names contains scalar.name) Some(scalar) else None
  }
      
  /**
   * TODO: Override to replace domain with an Index if it is not projected
   */
//  override def filterSample(sample: (Variable, Variable)): Option[(Variable, Variable)] = {
//    for (d <- filterVariable(sample._1); r <- filterVariable(sample._2)) yield (d,r)
//  }
  

  
  /*
   * TODO: do we need to filter Function by sample?
   * or just once at the model level
   * depends on whether Data is in Function or kids
   * replacing domain with Index needs to happen at Function (model) level
   *   need to know position/index
   *   replace domain set (Data?) with IndexSet
   */
  
  //TODO: preserve order of requested variables
  //TODO: exclude domain by replacing with IndexFunction
  //TODO: what if only part of nD domain is selected?
  //  only if it is a product set
  //TODO: return domain as IndexFunction if only it is projected?
  //TODO: deal with unnamed vars
  //TODO: deal with full name of nested vars

}

object ProjectionFilter {
  
  def apply(names: Seq[String]) = new ProjectionFilter(names)
}