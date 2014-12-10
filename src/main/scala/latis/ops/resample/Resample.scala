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
import latis.time.Time
import latis.util.DataUtils

abstract class Resample(domainName: String, set: DomainSet) extends Operation {
  //TODO: support nD domain
  //TODO: more nouny?
  //TODO: strategies: equals, nearest, linear, ... 
  //TODO: sanity check that domain set is ordered? enforce in DomainSet constructor
  //TODO: preserve type or convert to Reals?
  //TODO: allow RealSampledSet with integer domain?
  //TODO: consider type akin to visad instead of matching var name

  /**
   * Override this abstract method to implement the resampling strategy
   * given the bounding Samples and the desired domain sample.
   */
  protected def resample(sample1: Sample, sample2: Sample, domain: Variable): Sample
  
  override def applyToFunction(function: Function): Option[Variable] = {
    //apply if this Function has the domain type we are interested in
    if (function.getDomain.hasName(domainName)) {
      //Make new function with new samples, one for each element of the domain set

      val origSamples = PeekIterator(function.iterator)
      
      //get new sample for each domain value
      val newSamples = set.iterator.flatMap(data => {
        val domain = function.getDomain(data) //make variable of same type but with this Data
        getNewSample(origSamples, domain)
      })
      
      //set length in function's metadata
      val length = set.length
      val md = function.getMetadata + ("length" -> length.toString)
      val f = Function(function.getDomain, function.getRange, newSamples, md)
      Some(f)
      
    } else {
      //iterate over samples and apply to each
      val newSamples = function.iterator.flatMap(applyToSample(_))
      //TODO: munge metadata
      val f = Function(function, newSamples)
      Some(f)
    }
  }
  
  /**
   * Iterate through the samples until we find the bounding pair then delegate to 
   * the resampling strategy to create the new Sample.
   */
  private def getNewSample(samples: PeekIterator[Sample], domain: Variable): Option[Sample] = {
    //TODO: there's got to be a cleaner more FP way to do this
    var newSample: Sample = null
    
    //make sure we have started iterating
    var sample1 = samples.current match {
      case null => samples.next
      case sample: Sample => sample
    }
    var sample2 = samples.peek //will be null if we go beyond the end
    
    //assume numeric scalar domain, for now
    val domainValue = DataUtils.getDoubleValue(domain)
    
    //loop until we find the appropriate pair and make a sample or we run out
    while (newSample == null && sample2 != null) {
      val d1 = DataUtils.getDoubleValue(sample1.domain)
      val d2 = DataUtils.getDoubleValue(sample2.domain)
      
      //test if val is before
      //TODO: need to know order? assume ascending for now
      if (domainValue.compare(d1) < 0) return None
      //if domainValue is between (inclusive) these samples, independent of order
      if (domainValue.compare(d1) * domainValue.compare(d2) <= 0) {
        newSample = resample(sample1, sample2, domain)
      } else {
        //get next pair
        sample1 = samples.next
        sample2 = samples.peek
      }
    }
    
    if (newSample != null) Some(newSample)
    else None
  }

}
