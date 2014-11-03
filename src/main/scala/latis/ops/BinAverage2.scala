package latis.ops

import latis.dm._
import latis.util.MappingIterator
import scala.collection.mutable.ListBuffer
import scala.math._
import latis.util.PeekIterator
import latis.time.Time
import latis.metadata.Metadata
import latis.data.SampledData

class BinAverage2(binWidth: Double) extends Operation {
  //TODO: accept ISO 8601 time duration

  /*
   * TODO: try alternate approach for making new Function
   * http://mods-jira.lasp.colorado.edu:8080/browse/LATIS-184
   * 
   * Assume 1D domain.
   * If range is tuple, take first element, for now.
   * Nested Functions not yet supported.
   * Assume Function has SampledData
   * 
   * consider if we want domain and range iterators linked or just use indices
   * might as well iterate on SampleData
   * try to preserve domain set
   * or iterate over Samples
   *   seems more idiomatic, make use of types
   * what to do about length? leave undefined
   * 
   * get domain values as Seq
   *   segmentLength
   *   takeWhile, but leaves iterator invalid, must look at next to know it went too far
   */

  override def applyToFunction(f: Function): Option[Variable] = {
    val fit = PeekIterator(f.iterator)
    //If the function has no samples, return None for now.
    if (fit.isEmpty) None
    else {
      //get initial domain value
      //TODO: allow specification of initial value, e.g. so daily avg is midnight to midnight
      val startValue = getDomainValue(fit.peek)
      var nextValue = startValue

      //Make an iterator of new samples
      val sampleIterator = new PeekIterator[Sample] {
        def getNext = {
          nextValue += binWidth
          if (fit.isEmpty) null
          else {
            val binnedSamples = ListBuffer[Sample]()
            while (fit.hasNext && getDomainValue(fit.peek) < nextValue) binnedSamples += fit.next
            computeStatistics(binnedSamples) match {
              case Some(sample) => sample
              case None => getNext //skip invalid or empty bins, TODO: insert NaNs?
            }
          }
        }
      }

      val sampleTemplate = sampleIterator.peek
      Some(Function(sampleTemplate.domain, sampleTemplate.range, sampleIterator))
    }
  }
  
  /**
   * Extract the value of the domain variable in the given sample as a Double.
   * Error if not a 1D numeric Scalar. Time will be treated as unix ms.
   */
  private def getDomainValue(sample: Sample): Double = sample.domain match {
    case t: Time => t.getJavaTime.toDouble //TODO: only if type Text? user would need to know/use native units
    case Number(d) => d
    case _ => throw new Error("BinAverage supports only one dimensional numeric domains.")
  }
  
  private def computeStatistics(samples: Seq[Sample]): Option[Sample] = {
    //TODO: pattern match on range type so we can eventually extend this
    //TODO: make Function from these samples then apply mean, min, max...
    //scala Seq has: length, max, min, sum, (product)
    //Compute mean domain value
    
//return first sample for testing
    //TODO: compute stats
    if (samples.nonEmpty) Some(samples.head)
    else None
  }
  
  
}

object BinAverage2 extends OperationFactory {
  
  override def apply(args: Seq[String]): BinAverage2 = {
    if (args.length > 1) throw new UnsupportedOperationException("The BinAverage operation accepts only one argument")
    try {
      BinAverage2(args.head.toDouble)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The BinAverage requires a single numeric argument")
    }
  }
    
  def apply(binWidth: Double): BinAverage2 = new BinAverage2(binWidth)
}