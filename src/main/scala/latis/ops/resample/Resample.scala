package latis.ops.resample

import latis.data.set.DomainSet
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Number
import latis.dm.Sample
import latis.dm.Variable
import latis.dm.Real
import latis.data.Data
import latis.util.PeekIterator
import latis.data.IterableData
import latis.data.SampledData
import latis.ops.Operation

abstract class Resample(domainName: String, set: DomainSet) extends Operation {
  //TODO: support nD domain
  //TODO: more nouny?
  //TODO: strategies: equals, nearest, linear, ... 
  //TODO: sanity check that domain set is ordered? enforce in DomainSet constructor
  //TODO: preserve type or convert to Reals?
  //TODO: allow RealSampledSet with integer domain?
  
  /*
   * TODO: apply to samples vs getting domain set of Dataset
   * need 2 samples for strategies other than equality
   *   consider using Iterator.sliding when we need more than 2 samples
   * delegate to overridable function for diff strategies
   * 
   */
  
  protected def resample(sample1: Sample, sample2: Sample, domainValue: Double): Data
  
  override def applyToFunction(function: Function): Option[Variable] = {
    //apply if this Function has the domain type we are interested in
    if (function.getDomain.hasName(domainName)) {
      //Make new function with new samples, one for each element of the domain set
      //assume a 1D numeric domain for now
      val domain = function.getDomain
      
      val origSamples = PeekIterator(function.iterator)
          
      //Iterate over each datum in the new domain set.
      //Build IterableData for the range of the new Function
      val newData = set.iterator.map(data => {
        //get value of new domain sample as double
        val d = getDomainValue(domain, data)
        getResampledData(origSamples, d)
      })
      val rangeSampleSize = function.getRange.getSize
      val newRange = IterableData(newData, rangeSampleSize) //assumes same data types
      
      val data = SampledData(set, newRange)
      //TODO: munge metadata, especially length
      val f = Function(function, data)
      Some(f)
      
    } else {
      //apply to range to catch nested Functions
      applyToVariable(function.getRange)
    }
  }
  
  
  /**
   * 
   * Iterate on samples (using peek) until we find the pair we need.
   * Assumes samples are sorted.
   * This expects the data values to be in the Scalars of the Samples.
   */
  private def getResampledData(samples: PeekIterator[Sample], domainValue: Double): Data = {
    //TODO: consider order
    //TODO: consider using Iterator.sliding
    //TODO: use binary search, use blocks from iterator, estimate based on cadence...
    //TODO: deal with invalid samples, fill with missing values? empty data?
    /*
     * TODO: generalize
     * needs to be up to strategy to decide what to do with out of bounds
     * not all need 2 samples
     * maybe this is what needs to be overridden?
     * 
     */
    
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
    if (data.isEmpty) ??? //Note, causes warning in MappingIterator and drops sample
    
    data
  }
  

  
  
  /**
   * For domain Variables that contain the data.
   * E.g. from iterating on samples from a function.
   */
  def getDomainValue(domain: Variable): Double = domain match {
    case Number(d) => d
    case _ => throw new Error("The Resample Operation supports only numeric Scalar domains.")
  }
  
  /**
   * From the data using the domain as a template.
   */
  //TODO: use DataUtils?
  def getDomainValue(domain: Variable, data: Data): Double = {
    val bb = data.getByteBuffer
    domain match {
      case _: Real => bb.getDouble
      case _: Integer => bb.getLong.toDouble
      //TODO: Index?
      case _ => throw new Error("The Resample Operation supports only numeric Scalar domains.")
    }
  }
}