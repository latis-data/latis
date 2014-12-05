package latis.ops.resample

import latis.data.set.DomainSet
import latis.dm.Sample
import latis.dm.Variable

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
