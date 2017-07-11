package latis.ops

import latis.dm._
import latis.util.iterator.MappingIterator
import scala.collection.mutable.ListBuffer
import scala.math._
import latis.util.iterator.PeekIterator
import latis.time.Time
import latis.metadata.Metadata

class BinAverage(binWidth: Double) extends Operation {
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val it = new MappingIterator(f.iterator, (s: Sample) => Some(s))
    var binStart = it.peek.domain match {
      case Number(n) => n
    }
    val samples = ListBuffer[Sample]()
    while (it.hasNext) {
      val bin = makeBin(it, binStart)
      if (bin.nonEmpty) samples += binToSample(bin)
      
      binStart += binWidth
    }
    val md = Metadata(Map("length" -> samples.length.toString) ++ f.getMetadata.getProperties.filterKeys(_ != "length"))
    Some(Function(samples(0).domain, samples(0).range, samples.iterator))
  }
  
  def makeBin(it: PeekIterator[Sample], start: Double): Seq[Sample] = {
    val lb = new ListBuffer[Sample]()
    def b = it.peek.domain match {
      case Number(n) => n < start + binWidth
    }
    while (it.peek != null && b) lb += it.next
    lb.result
  }

  def binToSample(bin: Seq[Sample]): Sample = { //should empty bins be allowed?
    val count = bin.length
    val dom = bin(0).domain match {
      case t: Time => t match {
        case r: Number => Time(t.getMetadata, bin.map(_.domain.getNumberData.doubleValue).sum/count)
      }
      case n: Number => Real(n.getMetadata, bin.map(_.domain.getNumberData.doubleValue).sum/count)
    }
    
    val max = Real(Metadata("max"), bin.map(s => reduce(s.range).getNumberData.doubleValue).max)
    val min = Real(Metadata("min"), bin.map(s => reduce(s.range).getNumberData.doubleValue).min)
    val mean = bin.map(s => reduce(s.range).getNumberData.doubleValue).sum/count
    val stddev = Real(Metadata("stddev"), sqrt(bin.map(s => pow(reduce(s.range).getNumberData.doubleValue - mean, 2)).sum/(count - 1)))

    Sample(dom,Tuple(List(Real(reduce(bin(0).range).getMetadata, mean), min, max, stddev, Integer(Metadata("count"), count))))
  }
  
      
  //If the range is a tuple, just use the first element
  //TODO: could use some refactoring
  def reduce(v: Variable): Variable = v match {
    case s: Scalar => s
    case Tuple(vars) => vars.head
    case _: Function => throw new Error("Can't perform a bin average over a nested Function.")
  }
}

object BinAverage extends OperationFactory {
  
  override def apply(args: Seq[String]): BinAverage = {
    if (args.length > 1) throw new UnsupportedOperationException("The BinAverage accepts only one argument")
    try {
      BinAverage(args.head.toDouble)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The BinAverage requires a single numeric argument")
    }
  }
    
  def apply(binWidth: Double): BinAverage = new BinAverage(binWidth)
}
