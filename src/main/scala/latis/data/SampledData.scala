package latis.data

import latis.data.set.DomainSet
import latis.dm.Sample

class SampledData extends IterableData {
  //TODO: manage caching here? special subclass?
  
  def recordSize: Int = domainSet.recordSize + rangeData.recordSize
  
  private var _domain: DomainSet = null
  private var _range: IterableData = null
  
  //TODO: if null, iterate and cache both?
  def domainSet: DomainSet = _domain
  def rangeData: IterableData = _range

  //TODO: get length from domain set
  
  //private var _iterator: Iterator[SampleData] = null
  
  def iterator: Iterator[SampleData] = (_domain.iterator zip _range.iterator).map(p => SampleData(p._1,p._2))
//    _iterator match {
//    case null => (_domain.iterator zip _range.iterator).map(p => SampleData(p._1,p._2))
//    case _ => _iterator
//  }
    //(domainSet.iterator zip rangeData.iterator).map(p => p._1.concat(p._2))
}

object SampledData {
  
  def apply(domainData: IterableData, rangeData: IterableData): SampledData = SampledData(DomainSet(domainData), rangeData)
    
  def apply(domainSet: DomainSet, rangeData: IterableData): SampledData = {
    val sd = new SampledData
    sd._domain = domainSet
    sd._range = rangeData
    sd
  }
  
  //TODO: explore implications of duplicated Iterators (e.g. its caching)
  //TODO: should we manage our own internal iterator and caching?
  def apply(sampleIterator: Iterator[SampleData], sampleTemplate: Sample): SampledData = {
    val (dit,rit) = sampleIterator.duplicate
    val dset = DomainSet(IterableData(dit.map(_.domainData), sampleTemplate.domain.getSize))
    val rdata = IterableData(rit.map(_.rangeData), sampleTemplate.range.getSize)
    SampledData(dset, rdata)
  }
  
  //TODO: need to peek to get recordSize
//  def apply(sampleIterator: Iterator[SampleData]) = {
//    val sd = new SampledData
//    sd._iterator = sampleIterator
//    sd
//  }
}