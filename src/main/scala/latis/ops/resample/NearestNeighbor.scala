package latis.ops.resample

import latis.data.set.DomainSet
import latis.dm.Sample
import latis.dm.Variable
import latis.util.StringUtils
import latis.data.set.RealSampledSet
import latis.data.set.TextSampledSet

class NearestNeighbor(domainName: String, set: DomainSet) extends Resample(domainName, set) {
  
  /**
   * Nearest neighbor.
   * Make data for new sample's range
   * Don't need to worry about range type since we just reuse the nearest sample.
   */
  override protected def resample(sample1: Sample, sample2: Sample, domain: Variable): Sample = {
    //nearest neighbor
    //assume numeric scalar domain, for now
    val domainValue = getDomainValue(domain)
    val d1 = getDomainValue(sample1.domain)
    val d2 = getDomainValue(sample2.domain)
    val dd1 = Math.abs(domainValue - d1)
    val dd2 = Math.abs(domainValue - d2)
    
    //Note: tie goes to latter, equivalent to rounding up (for ascending order).
    val range = if (dd1 < dd2) sample1.range else sample2.range
    
    Sample(domain, range)
  }
  
}

object NearestNeighbor {
  
  def apply(vname: String, value: String): NearestNeighbor = {
    /*
     * TODO: need to construct domain set from value
     * need dataset to get type
     * should Resample not take a domain set arg?
     * array of values? string, long, double
     * could map those to domain set (Data) type
     * but we want to be able to use string for all cases (e.g. constraint expression)
     * need to convert time units from iso
     * 
     * hack it based on value, for now
     */
    val set = if(StringUtils.isNumeric(value)) RealSampledSet(List(value.toDouble))
    else TextSampledSet(List(value))
    
    new NearestNeighbor(vname, set)
  }
  
}
