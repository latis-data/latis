package latis.ops

import latis.dm._
import latis.util.MappingIterator
import scala.collection.mutable.ListBuffer
import scala.math._
import latis.util.PeekIterator
import latis.time.Time
import latis.metadata.Metadata

class BinAverage(binWidth: Double) extends Operation {
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val it = new MappingIterator(f.iterator, (s: Sample) => Some(s))
    var binStart = it.peek.domain match {
      case t: Time => t.getJavaTime
      case n: Number => n.getNumberData.doubleValue
    }
    val samples = ListBuffer[Sample]()
    while(it.hasNext) {
      val bin = makeBin(it, binStart)
      if(bin.nonEmpty) samples += binToSample(bin)
      
      binStart += binWidth
    }
    val md = Metadata(Map("length" -> samples.length.toString) ++ f.getMetadata.getProperties.filterKeys(_ != "length"))
    Some(Function(samples(0).domain, samples(0).range ,samples.iterator))
  }
  
  def makeBin(it: PeekIterator[Sample], start: Double): Seq[Sample] = {
    val lb = new ListBuffer[Sample]()
    def b = it.peek.domain match {
      case t: Time => t.getJavaTime.doubleValue < start + binWidth
      case n: Number => n.getNumberData.doubleValue < start + binWidth
    }
    while(it.peek != null && b) lb += it.next
    lb.result
  }

  def binToSample(bin: Seq[Sample]): Sample = {//should empty bins be allowed?
    val count = bin.length
    val dom = bin(0).domain match {
      case t: Time => t match {
        case r: Number => Time(t.getMetadata, bin.map(_.domain.getNumberData.doubleValue).sum/count)
        case t: Text => Time(t.getMetadata, bin.map(_.domain.asInstanceOf[Time].getJavaTime).sum/count)
      }
      case n: Number => Real(n.getMetadata, bin.map(_.domain.getNumberData.doubleValue).sum/count)
    }
    val max = Real(Metadata("max"), bin.map(_.range.getNumberData.doubleValue).max)
    val min = Real(Metadata("min"), bin.map(_.range.getNumberData.doubleValue).min)
    val mean = bin.map(_.range.getNumberData.doubleValue).sum/count
    val stddev = Real(Metadata("stddev"), sqrt(bin.map(s => pow(s.range.getNumberData.doubleValue - mean, 2)).sum/(count - 1)))

    Sample(dom,Tuple(List(Real(bin(0).range.getMetadata, mean), min, max, stddev, Integer(Metadata("count"), count))))
  }
}