package latis.data

import latis.data.set.DomainSet

class SampledData(domainSet: DomainSet, rangeData: IterableData) extends IterableData {
  
  def recordSize: Int = domainSet.recordSize + rangeData.recordSize
  
  def iterator: Iterator[Data] = (domainSet.iterator zip rangeData.iterator).map(p => p._1.concat(p._2))
}