package latis.ops

import latis.dm._
import latis.util.iterator.MappingIterator
import scala.collection.Map
import scala.collection.mutable.ListBuffer
import scala.math._
import latis.util.iterator.PeekIterator
import latis.time.Time
import latis.metadata.Metadata
import latis.data.SampledData
import latis.ops.filter.ExcludeMissing

/**
 * Assume 1D domain.
 * If range is tuple, take first element, for now.
 * Nested Functions not yet supported.
 */
class BinAverageByWidth(binWidth: Double, startVal: Double = Double.NaN) extends Operation {
  //TODO: accept ISO 8601 time duration
  //TODO: start with min of time coverage instead of 1st sample
  //TODO: deal with 'length' metadata
  //TODO: deal with missing values
  //TODO: take domain var arg so we can bin nested functions, akin to integration
  
  if (binWidth <= 0) throw new Error("Bin average must have a positive bin width.")
  
  /**
   * Get bin width via getter so it can be overridden.
   */
  def getBinWidth = binWidth
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val fit = PeekIterator(f.iterator)
    //If the function has no samples, make a new Function around this iterator (since we can't reuse the orig)
    if (fit.isEmpty) Some(Function(f, fit))
    else {
      //Get initial domain value so we know where to start,
      //default to first sample in the data if no startVal was provided in constructor
      var nextValue = if (startVal.isNaN) getDomainValue(fit.peek) else startVal
      
      val domainMetadata = f.getDomain.getMetadata
      val rangeMetadata = reduce(f.getRange).getMetadata //first scalar

      //Make an iterator of new samples
      val sampleIterator = new PeekIterator[Sample] {
        def getNext = {
          nextValue += getBinWidth
          if (fit.isEmpty) null
          else {
            //accumulate the samples for this bin
            val binnedSamples = ListBuffer[Sample]()
            while (fit.hasNext && getDomainValue(fit.peek) < nextValue) binnedSamples += fit.next
            
            //create domain with bin center as its value, reuse original metadata
            //TODO: munge metadata
            val domainValue = nextValue - 0.5 * getBinWidth //bin center
            val domain = fit.current.domain match {
              //TODO: use "copy"
              case _: Time => Time(domainMetadata, domainValue)
              case _ => Real(domainMetadata, domainValue)
            } 
            
            //compute statistics on range values
            val range = computeStatistics(binnedSamples) match {
              case Some(range) => range
              case None => {
                //fill empty bin with missing_value or NaN
                val fillValue = rangeMetadata.get("missing_value") match {
                  case Some(s) => s.toDouble
                  case None => Double.NaN
                }
                //TODO: use scalar.getFillValue
                //TODO: or call getNext if we want to skip empty bins
                val mean = Real(rangeMetadata, fillValue)
                val min  = Real(Metadata("min"), fillValue)
                val max  = Real(Metadata("max"), fillValue)
                val count = Real(Metadata("count"), 0)
                Tuple(mean, min, max, count) //TODO: add metadata, consider model for bins
              }
            }
            
            Sample(domain, range)
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
    case Number(d) => d
    case _ => throw new Error("BinAverage supports only one dimensional numeric domains.")
  }
  
  private def computeStatistics(samples: Seq[Sample]): Option[Tuple] = {
    //Assume all Samples have the same type
    //TODO: pattern match on range type so we can eventually extend this
    //TODO: make Function from these samples then apply mean, min, max?
    //TODO: consider CF binning conventions
    
    if (samples.isEmpty) None
    else {
      //process only the first scalar, for now
      val rangeTemplate = reduce(samples.head.range)
      
      //get data as a Map: name -> array of values
      val data = samplesToDoubleMap(samples)
      val values = data.get(rangeTemplate.getName) match {
        case Some(vs) => vs
        case None => return None
      }
      
      //if the original data was already binned, use the counts to weigh the bins and ignore empty bins
      val counts: Array[Double] = data.get("count") match {
        case Some(cs) => cs //counts for each bin
        case None => Array.fill(values.length)(1) //treat regular samples as bins of size 1
      }
      
      //total number of samples going into the stats
      val n = counts.sum 
      val count = Real(Metadata("count"), n)
      
      //weigh by counts per bin
      val meanValue = values.zip(counts).foldLeft(0.0)((s,p) => s + p._1 * p._2 / n) 
      val mean = Real(rangeTemplate.getMetadata, meanValue)
      
      //if the original data was already binned (i.e. has min and max value) then use them
      //but not if the bin was empty
      val minValue = data.get("min") match {
        case Some(ms) => {
          val vs = ms.zip(counts).filter(_._2 != 0).map(_._1)
          if (vs.isEmpty) Double.NaN
          else vs.min
        }
        case None => values.min
      }
      val min = Real(Metadata("min"), minValue)
      
      val maxValue = data.get("max") match {
        case Some(ms) => {
          val vs = ms.zip(counts).filter(_._2 != 0).map(_._1)
          if (vs.isEmpty) Double.NaN
          else vs.max
        }
        case None => values.max
      }
      val max = Real(Metadata("max"), maxValue)
      
      Some(Tuple(mean, min, max, count))
    }
  }
  
  //If the range is a tuple, just use the first element
  private def reduce(v: Variable): Scalar = v match {
    case s: Scalar => s
    case Tuple(vars) => reduce(vars.head)
    case _: Function => throw new Error("Can't perform a bin average over a nested Function.")
  }
  
  private def samplesToDoubleMap(samples: Seq[Sample]): Map[String, Array[Double]] = {
    //make a dataset from these samples then op on it
    val ds = Dataset(Function(samples))
    //exclude samples with missing data
    val ds2 = ExcludeMissing()(ds)
    
    
    ds2.toDoubleMap
  }
}


object BinAverageByWidth extends OperationFactory {
  
  override def apply(args: Seq[String]): BinAverageByWidth = {
    if (args.length > 2) throw new UnsupportedOperationException("The BinAverage operation only accepts up to two arguments")
    try {
      BinAverageByWidth(args.head.toDouble)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The BinAverage requires numeric arguments")
    }
  }
    
  def apply(binWidth: Double, startVal: Double = Double.NaN): BinAverageByWidth = new BinAverageByWidth(binWidth, startVal)
}
