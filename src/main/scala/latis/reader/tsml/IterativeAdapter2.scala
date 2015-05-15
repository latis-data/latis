package latis.reader.tsml

import scala.Option.option2Iterable
import scala.collection.Map
import scala.collection.TraversableOnce.flattenTraversableOnce

import latis.data.Data
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.reader.tsml.ml.Tsml

/**
 * Base class for Adapters for data sources that have 'record' semantics.
 */
abstract class IterativeAdapter2[R](tsml: Tsml) extends TsmlAdapter(tsml) {
  //R is the type of record
  
  def getRecordIterator: Iterator[R] 
  
  def parseRecord(record: R): Option[Map[String, Data]] 
  
  def dataMapIteratorToSampleIterator(dmit: Iterator[Map[String, Data]], sampleTemplate: Sample): Iterator[Sample] = {
    val recordsPerSample = innerLength(sampleTemplate)//how many records are needed to make one Sample
    val dt = sampleTemplate.domain
    val rt = sampleTemplate.range
    recordsPerSample match {
      case 1 => { //basic function
        dmit.map(dm => dataMapToSample(dm, sampleTemplate))
      }
      case _ => { //nested function
        def dataMapsToSample(maps: Seq[Map[String,Data]]) = { //recursively creates inner Function
          val d = dt(maps(0)(dt.getName))
          val rf = rt.findFunction.get
          val r = Function(rf.getDomain, rf.getRange, dataMapIteratorToSampleIterator(maps.toIterator, rf.getSample), rf.getMetadata)
          Sample(d,r)
        }
        dmit.grouped(recordsPerSample).map(dataMapsToSample(_))
      }
    }
  }
   /**
    * Returns the product of the lengths of the Functions nested in the given Sample.
    * This is how many records are needed to make one Sample.
    */
  def innerLength(s: Sample): Int = s.range.findFunction match {
    case None => 1
    case Some(f) => f.getLength * innerLength(f.getSample)
  }
  
  /**
   * Extract a Sample following sampleTemplate from the Data Map dm
   */
  def dataMapToSample(dm: Map[String, Data], sampleTemplate: Sample): Sample = {
    val dt = sampleTemplate.domain
    val rt = sampleTemplate.range
    val d = dt match {
      case i: Index => Index()
      case s: Scalar => dt(dm(dt.getName))
    }
    val r = rt match {
      case s: Scalar => s(dm(s.getName))
      case t: Tuple => Tuple(rt.toSeq.map(x => x(dm(x.getName))))
    }
    Sample(d, r)
  }
      
  override def makeFunction(f: Function): Option[Function] = {
    val template = Sample(f.getDomain, f.getRange)
    makeSample(template) match {
      case Some(sample) => {
        val it: Iterator[Sample] = dataMapIteratorToSampleIterator(getRecordIterator.map(parseRecord(_)).flatten, sample)
        Some(Function(sample.domain, sample.range, it, f.getMetadata))
      }
      case None => None
    }
  }
  
}
