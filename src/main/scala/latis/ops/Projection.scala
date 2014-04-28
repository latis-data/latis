package latis.ops

import latis.dm._
import latis.util.DataUtils
import latis.util.RegEx._
import latis.data.SampledData
import latis.data.set.IndexSet
import latis.util.PeekIterator2
import latis.data.Data
import latis.data.SampleData
import latis.data.IterableData

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
    //TODO: apply projection order?
    val vars = tuple.getVariables.flatMap(applyToVariable(_))
    if (vars.length == 0) None
    else Some(Tuple(vars)) //TODO: metadata
  }
  
  /**
   * Since we may replace a dropped domain with Index, we may need to make a new SampledFunction
   * with SampledData with an IndexSet for the domain.
   */
  override def applyToFunction(function: Function): Option[Variable] = {
    
    val sample1 = Sample(function.getDomain, function.getRange)
    
    val sample2 = applyToSample(sample1) match {
      case Some(s) => s
      case None => throw new Error("Failed to project the sample: " + sample1)
    }
 
    /*
     * TODO: what if orig f had index, or any DomainSet for that matter?
     * no harm done for Index
     * ++ reuse DomainSet if it is projected
     * 
     * TODO: what if function is a WrappedFunction without data?
     * do we dare convert Samples back to Data?
     * via f.getDataIterator?
     * could we just op on samples?
     * back to same index set problem
     */
    
    val Sample(d,r) = sample2
    val sampledData = if (d.isInstanceOf[Index]) {
      //Only need to process range, but orig data includes all
      val tmp2 = sample2.range
      val f = (data: SampleData) => Some(DataUtils.reshapeData(data, sample1, tmp2))
      val dataIt = new PeekIterator2(function.getDataIterator, f)
      SampledData(IndexSet(), IterableData(dataIt, tmp2.getSize))
    } else {
      val f = (data: SampleData) => Some(DataUtils.reshapeSampleData(data, sample1, sample2))
      val dataIt = new PeekIterator2(function.getDataIterator, f)
      SampledData(dataIt, sample2)
    }
    
    Some(Function(d, r, sampledData))
  }

/*
 * TODO: consider doing the data munging here so the WrappedFunction starts with the correct type
 * apply projection order while we are at it
 * if we do that all here, then don't even need WrappedFunction
 * should we define a function to map the Data then wrap it in an iterator and sampled data? (see IterativeAdapter)
 * 
 * DataUtil reshapeData(data, sample1, sample2): SampledData
 *   dataToDataMap(data, sample): Map[String,Data]
 *   no need to go all the way to Samples just to come back to data
 *   if we did go to Samples, then we could do like WrappedFunction, applyToSample, but need to be careful about how we manage type change
 *  *would waste time converting non-projected data to Vars, couldn't avoid buggy data by not projecting
 *  dataToDataMap then 'reshape' here?
 *  revisit when we start reshaping nD domains
 *  or other Ops that need to muck with data
 * 
 * call dataToDataMap on entire Function?
 * or iterate around it?
 * iterate on SampledData to get SampleData-s
 * consider nested Functions
 * 
 * make PeekIterator2(function.getData.iterator, f)
 * function to map data for an original sample to data for the new sample
 * based on variable size, without creating Variables
 * use dataToDataMap
 * within reshapeData?
 */
//      //function to map data for an original sample to data for the new sample
//      //based on variable size, without creating Variables
//      val f = (data: SampleData) => Some(DataUtils.reshapeData(data, sample1, sample2))
//      
//      val dataIt = new PeekIterator2(function.getData.iterator, f)
//      val data = SampledData(dataIt, sample2)
//      
//      Some(Function(d, r, data))
//      //Some(WrappedFunction(function, this))
//    }
//   
//    ???
//    //Some(ProjectedFunction(function, this))
  
  
  
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
