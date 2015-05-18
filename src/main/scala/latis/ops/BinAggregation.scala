package latis.ops

import latis.dm._
import latis.util.PeekIterator
import latis.time.Time
import scala.collection.mutable.ListBuffer
import latis.dm.implicits._
import latis.data.Data
import scala.math.Ordering.String._
import latis.metadata.Metadata

/**
 * Puts the original Function into bins where each domain value is within
 * 'binWidth' of every other domain value in the bin. Then reduces each bin 
 * to a single Sample according to 'agg' and makes a new Function. 
 */
class BinAggregation(agg: Seq[Sample] => Sample, binWidth: Double) extends Operation {
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val it = PeekIterator(f.iterator)
    val binit = new Iterator[Iterator[Sample]] {
	  /**
	   * Extract the value of the domain variable in the given sample as a Double.
	   * Error if not a 1D numeric Scalar. Time will be treated as unix ms.
	   */
	  private def getDomainValue(sample: Sample): Double = sample.domain match {
	    case t: Time => t.getJavaTime.toDouble //TODO: only if type Text? user would need to know/use native units
	    case Number(d) => d
	    case _ => throw new Error("BinAggregation supports only one dimensional numeric domains.")
	  }
	  var stopVal = getDomainValue(it.peek)
	  def hasNext = it.hasNext
	  def next = {
	    val sit = new Iterator[Sample] {
	      stopVal += binWidth
	      def hasNext = it.hasNext && getDomainValue(it.peek) < stopVal
	      def next = it.next
	    }
	    sit.isEmpty match {
	      case true => next //skip empty bins
	      case false => sit
	    }
	  }
	}
    
    val md = f.getMetadata("length") match {
      case None => f.getMetadata //still don't know length
      case Some(l) => Metadata(f.getMetadata.getProperties - "length") //don't know length anymore
    }
    val fit = PeekIterator(binit.map(agg.compose(_.toSeq)))
    Some(Function(fit.peek.domain, fit.peek.range, fit, md))
  }

}

object BinAggregation {
  def apply(agg: Seq[Sample] => Sample, binwidth: Double) = new BinAggregation(agg, binwidth)
  
  val SUM = (ss: Seq[Sample]) => ss.reduceLeft((s1,s2) => Sample((s1.domain+s2.domain).unwrap, (s1.range+s2.range).unwrap))
  val AVERAGE = (ss: Seq[Sample]) => {
    val sum = SUM(ss)
    Sample((sum.domain / ss.length).unwrap, (sum.range / ss.length).unwrap)
  }
  val MIN = (ss: Seq[Sample]) => {
    def compare(v1: Variable , v2: Variable): Variable = (v1, v2) match {
      case (n1 @ Number(d1), n2 @ Number(d2)) => n1(Data(d1 min d2))
      case (t1 @ Text(s1), t2 @ Text(s2)) => t1(Data(min(s1, s2)))
      case (t1 @ Tuple(vs1), t2 @ Tuple(vs2)) => Tuple(vs1.zip(vs2).map(p => compare(p._1, p._2)), t2.getMetadata)
    }
    ss.reduceLeft((s1,s2) => Sample(compare(s1.domain, s2.domain), compare(s1.range, s2.range)))
  }
  val MAX = (ss: Seq[Sample]) => {
    def compare(v1: Variable , v2: Variable): Variable = (v1, v2) match {
      case (n1 @ Number(d1), n2 @ Number(d2)) => n1(Data(d1 max d2))
      case (t1 @ Text(s1), t2 @ Text(s2)) => t1(Data(max(s1, s2)))
      case (t1 @ Tuple(vs1), t2 @ Tuple(vs2)) => Tuple(vs1.zip(vs2).map(p => compare(p._1, p._2)), t2.getMetadata)
    }
    ss.reduceLeft((s1,s2) => Sample(compare(s1.domain, s2.domain), compare(s1.range, s2.range)))
  }
  val COUNT = (ss: Seq[Sample]) => Sample(ss(0).domain, Integer(Metadata("length"), ss.length))

}
