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
import latis.util.DataUtils

class NearestNeighbor(domainName: String, set: DomainSet) extends Resample(domainName, set) {
  
  /**
   * Nearest neighbor.
   * Make data for new sample's range
   * Don't need to worry about range type since we just reuse the nearest sample.
   */
  override protected def resample(sample1: Sample, sample2: Sample, domainValue: Double): Data = {
    //dnearest neighbor
    //assume numeric scalar domain, for now
    val d1 = getDomainValue(sample1.domain)
    val d2 = getDomainValue(sample2.domain)
    val dd1 = Math.abs(domainValue - d1)
    val dd2 = Math.abs(domainValue - d2)
    //Note: tie goes to latter, equivalent to rounding up (for ascending order).
    
    //TODO: tuple range wont have data
    /*
     * should we just make a new sample instead of trying to preserve the domain set?
     */
    val data= if (dd1 < dd2) DataUtils.buildDataFromVariable(sample1.range)
    else DataUtils.buildDataFromVariable(sample2.range)
    
    data
  }
  
}
