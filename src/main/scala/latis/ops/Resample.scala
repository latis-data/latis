package latis.ops

import latis.data.set.DomainSet
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Number
import latis.dm.Sample
import latis.dm.Variable
import latis.dm.Real
import latis.data.Data
import latis.util.PeekIterator
import latis.dm.Scalar
import latis.data.IterableData
import latis.data.SampledData

class Resample(domainName: String, set: DomainSet) extends Operation {
  //TODO: support nD domain
  //TODO: construct from domain var name?
  //TODO: more nouny?
  //TODO: strategies: equals, nearest, linear, ...  maybe its own package?
  //TODO: sanity check that domain set is ordered? enforce in DomainSet constructor
  //TODO: preserve type or convert to Reals?
  
  /*
   * TODO: apply to samples vs getting domain set of Dataset
   * need 2 samples for strategies other than equality
   *   consider using Iterator.sliding when we need more than 2 samples
   * delegate to overridable function for diff strategies
   * 
   */
  
  override def applyToFunction(function: Function): Option[Variable] = {
    //apply if this Function has the domain type we are interested in
    if (function.getDomain.hasName(domainName)) {
      val domain = function.getDomain
      //Make new function with new samples, one for each element of the domain set
      //assume a 1D numeric domain for now
      //TODO: use DomainSet of Function vs Iterating over Samples
      
      val origSamples = PeekIterator(function.iterator)
          
      //iterate over each datum in the new domain set
      //Build IterableData for the range of the new Function
      val newData = set.iterator.map(data => {
        //get value of new domain sample as double
        val d = getDomainValue(domain, data)
        getResampledData(origSamples, d)
      })
      val newRange = IterableData(newData, 8) //assume doubles
      
      val data = SampledData(set, newRange)
      //TODO: munge metadata, especially length
      //TODO: change types to Real?
      Some(Function(function, data))
      
    } else {
      //TODO: apply to range (super?) to catch nested Functions
      ???
    }
  }
  

  /*
   * Given two samples and the desired domain value, get the new range data.
   * If the domain value is not in the interval then return None. Thus no extrapolation.
   * This just determines if this pair of samples is relevant then delegates to an overridable method
   * to perform the resampling.
   * Note, the data values should be in the Scalars of the Samples.
   */
  
  /**
   * 
   * Iterate on samples (using peek) until we find the pair we need.
   * Assumes samples are sorted.
   */
  private def getResampledData(samples: PeekIterator[Sample], domainValue: Double): Data = {
    //TODO: consider order
    //TODO: consider using Iterator.sliding
    //TODO: use binary search, use blocks from iterator, estimate based on cadence...
    
    var data: Data = Data.empty
    
    while (samples.hasNext && data.isEmpty) {
      val sample1 = samples.next
      val sample2 = samples.peek
      //assume numeric scalar domain, for now
      val d1 = getDomainValue(sample1.domain)
      val d2 = getDomainValue(sample2.domain)
      
      //if domainValue is between (inclusive) these samples
      if (domainValue.compare(d1) * domainValue.compare(d2) <= 0) data = resample(sample1, sample2, domainValue)
    }
    
    //TODO: If no data found (e.g. domainValue outside of range) return missing value
    //TODO: consider out of range, use endpoints for nearest?
    if (data.isEmpty) ???
    
    data
  }
  
  /**
   * Nearest neighbor.
   * Make data for new sample's range
   */
  protected def resample(sample1: Sample, sample2: Sample, domainValue: Double): Data = {
    //dnearest neighbor
    //assume numeric scalar domain, for now
    val d1 = getDomainValue(sample1.domain)
    val d2 = getDomainValue(sample2.domain)
    val dd1 = Math.abs(domainValue - d1)
    val dd2 = Math.abs(domainValue - d2)
    //Note: tie goes to latter, equivalent to rounding up (for ascending order).
    if (dd1 < dd2) sample1.range.getData
    else sample2.range.getData
  }
  
  
  /**
   * For domain Variables that contain the data.
   * E.g. from iterating on samples from a function.
   */
  private def getDomainValue(domain: Variable): Double = domain match {
    case Number(d) => d
    case _ => throw new Error("The Resample Operation supports only numeric Scalar domains.")
  }
  
  /**
   * From the data using the domain as a template.
   */
  //TODO: use DataUtils?
  private def getDomainValue(domain: Variable, data: Data): Double = {
    val bb = data.getByteBuffer
    domain match {
      case _: Real => bb.getDouble
      case _: Integer => bb.getLong.toDouble
      //TODO: Index?
      case _ => throw new Error("The Resample Operation supports only numeric Scalar domains.")
    }
  }
}