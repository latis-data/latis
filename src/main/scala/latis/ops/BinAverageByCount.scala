package latis.ops

import latis.dm._
import latis.util.MappingIterator
import scala.collection.Map
import scala.collection.mutable.ListBuffer
import scala.math._
import latis.util.PeekIterator
import latis.time.Time
import latis.metadata.Metadata
import latis.data.SampledData

/**
 * Bin the outer Function of the given Dataset with equal size bins
 * such that the resulting number of bins is binCount.
 * Assume 1D Time domain, for now.
 * If range is tuple, take first element, for now.
 * Nested Functions not yet supported.
 */
class BinAverageByCount(binCount: Int) extends BinAverageByWidth(1) {
  //TODO: better name: might imply that each bin has the same number of samples
  //TODO: better way than passing dummy value to super constructor
  
  if (binCount <= 0) throw new Error("Bin average must have a positive number of bins.")
  
  /*
   * TODO: support 'number of bins' argument
   * get time range from dataset metadata to compute binWidth?
   * add hasDefiniteSize (like scala Seq) to Function and/or Dataset or any Variable?
   * or should this use binWidth arg? 
   *   or support both? diff name? int vs float?
   *   2 ops, one extends other
   * check if function hasDefiniteSize? or a DomainSet
   * could we use the domain set without having to read all data?
   *   make use of index to map domain set to range IterableData?
   * 
   * use coverage metadata
   * 'range' in the domain variable would be more general that using temporal coverage
   * for time, should we use the iso constraints or convert to native units?
   *   we currently expect bin width in ms
   * if we had coverage in dataset metadata it would be more accessible, so we don't have to peek at iterator
   *   support only time for now?
   *   default end to NOW?
   * TODO: start with min time instead of 1st sample
   */
  
  //milliseconds since 1970
  private var time_min: Long = Long.MinValue
  private var time_max: Long = Long.MaxValue
  
  //TODO: will this override the binWidth arg passed to super constructor?
  //  need to use getter?
  private lazy val _binWidth = {
    val w = (time_max - time_min).toDouble / binCount
    if (w > 0) w
    else throw new Error("Bin width must be greater than 0.")
  }
  override def getBinWidth = _binWidth
  
  /**
   * Override just to get Dataset time range from metadata.
   * Only TelemetryAdapter adds this right now.
   */
  //TODO: should we be able to ask the Operation for the Dataset it is working on?
  override def apply(dataset: Dataset): Dataset = {
    val md = dataset.getMetadata
    time_min = md.get("time_min") match {
      case Some(s) => Time.fromIso(s).getJavaTime
      case None => throw new Error("BinAverageByCount requires that Dataset have 'time_min' metadata.")
    }
    time_max = md.get("time_max") match {
      case Some(s) => Time.fromIso(s).getJavaTime
      case None => throw new Error("BinAverageByCount requires that Dataset have 'time_max' metadata.")
      //TODO: default to NOW?
    }
    
    super.apply(dataset)
  }

}


object BinAverageByCount extends OperationFactory {
  
  override def apply(args: Seq[String]): BinAverageByCount = {
    if (args.length > 1) throw new UnsupportedOperationException("The BinAverageByCount operation accepts only one argument")
    try {
      BinAverageByCount(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The BinAverageByCount requires a single integer argument")
    }
  }
    
  def apply(binCount: Int): BinAverageByCount = new BinAverageByCount(binCount)
}
