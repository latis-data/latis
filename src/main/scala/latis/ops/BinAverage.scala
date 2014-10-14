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
    var binStart = it.peek.domain.getNumberData.doubleValue
    var bin = makeBin(it, binStart)
    val samples = ListBuffer[Sample]()
    while(bin.nonEmpty) {
      samples += binToSample(bin)
      
      binStart += binWidth
      bin = makeBin(it, binStart)
    }
    val md = Metadata(Map("length" -> samples.length.toString) ++ f.getMetadata.getProperties.filterKeys(_ != "length"))
    Some(Function(samples(0).domain, samples(0).range ,samples.iterator))
  }
  
  def makeBin(it: PeekIterator[Sample], start: Double): Seq[Sample] = {
    val lb = new ListBuffer[Sample]()
    while(it.peek != null && it.peek.domain.getNumberData.doubleValue < start + binWidth) lb += it.next
    lb.result
  }

  def binToSample(bin: Seq[Sample]): Sample = {
    val count = bin.length
    val dom = bin(0).domain match {
      case t: Time => Time(t.getMetadata, bin.map(_.domain.getNumberData.doubleValue).sum/count)
      case r: Real => Real(r.getMetadata, bin.map(_.domain.getNumberData.doubleValue).sum/count)
      case i: Integer => Integer(i.getMetadata, bin.map(_.domain.getNumberData.longValue).sum/count)
    }
    val max = Real(Metadata("max"), bin.map(_.domain.getNumberData.doubleValue).max)
    val min = Real(Metadata("min"), bin.map(_.domain.getNumberData.doubleValue).min)
    val mean = bin.map(_.domain.getNumberData.doubleValue).sum/count
    val stddev = Real(Metadata("stddev"), sqrt(bin.map(s => pow(s.domain.getNumberData.doubleValue - mean, 2)).sum/(count - 1)))

    Sample(dom,Tuple(List(Real(bin(0).range.getMetadata, mean), min, max, stddev, Integer(Metadata("count"), count))))
  }
}