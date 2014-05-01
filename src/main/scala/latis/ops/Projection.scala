package latis.ops

import latis.dm._
import latis.util.DataUtils
import latis.util.RegEx._
import latis.data.SampledData
import latis.data.set.IndexSet
import latis.util.MappingIterator
import latis.data.Data
import latis.data.SampleData
import latis.data.IterableData
import latis.data.seq.DataSeq

/**
 * Exclude variables not named in the given list.
 */
class Projection(val names: Seq[String]) extends Operation {
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
   * Used to get the new data types, not to process each sample.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    val pd = applyToVariable(sample.domain)
    val pr = applyToVariable(sample.range)
    (pd,pr) match {
      case (Some(d), Some(r)) => Some(Sample(d,r))
      case (None, Some(r))    => Some(Sample(Index(), r)) //no domain, so replace it with Index
      case (Some(d), None)    => Some(Sample(Index(), d)) //no range, so make domain the range of an index function
      case (None, None) => ??? //TODO: nothing projected, could return Null but is it an error if we get this far?
    }
  }
  
  /**
   * Apply to all elements in the Tuple. 
   * Return None if all elements are excluded.
   */
  override def applyToTuple(tuple: Tuple): Option[Tuple] = {
    //Apply projection order. If this is a stand-alone Tuple, the data should already live within its elements.
    //Otherwise it is part of a Function which should handle the data restructuring.
    //This won't affect the domain variables which will be first (and establish ordering for samples of multidimensional domains)
    val vars = names.flatMap(tuple.getVariableByName(_)).flatMap(applyToVariable(_))
    //val vars = tuple.getVariables.flatMap(applyToVariable(_)) //orig var order
    if (vars.length == 0) None
    else Some(Tuple(vars)) //TODO: metadata
  }
  
  /**
   * Since we may be excluding some Data, make a new SampledFunction with new SampledData
   * that corresponds to the projected Variables. 
   * Note, applying a projection one sample at a time via a WrappedFunction is problematic
   * since the model of the dataset may change (not to mention getting the right index values
   * when the domain is not projected).
   */
  override def applyToFunction(function: Function): Option[Variable] = {
    //original sample type
    val sample1 = Sample(function.getDomain, function.getRange)
    
    //new sample type
    val sample2 = applyToSample(sample1) match {
      case Some(s) => s
      case None => throw new Error("Failed to project the sample: " + sample1)
    }
    
    //expose the domain and range types of the new dataset
    val Sample(d,r) = sample2
    
    //Deal with case where domain type is Index.
    val sampledData = if (d.isInstanceOf[Index]) {
      //Only need to process range, use IndexSet for domain.
      val f = (data: SampleData) => Some(DataUtils.reshapeData(data, sample1, r))
      val dataIt = new MappingIterator(function.getDataIterator, f)
      //SampledData(IndexSet(), IterableData(dataIt, r.getSize)) //TODO: bug trying to build on iterator
      val idata = DataSeq(dataIt.toList)
      SampledData(IndexSet(), idata)
    } else {
      //process all data
      //TODO: try to preserve original DomainSet
      val f = (data: SampleData) => Some(DataUtils.reshapeSampleData(data, sample1, sample2))
      val dataIt = new MappingIterator(function.getDataIterator, f)
      SampledData(dataIt, sample2)
    }
    
    Some(Function(d, r, data = sampledData))
  }


  override def toString = names.mkString(",")
}

object Projection {
  
  def apply(names: Seq[String]) = new Projection(names)
    
  def apply(expression: String): Projection = expression.trim match {
    //case PROJECTION.r(names) => //Note, regex match will only expose first and last
    case s: String if (s matches PROJECTION) => Projection(s.split(""",\s*""")) //Note, same delimiter used in PROJECTION def
    case _ => throw new Error("Failed to make a Projection from the expression: " + expression)
  }
  
  //Extract the list of projected variable names
  def unapply(proj: Projection) = Some(proj.names)

}
