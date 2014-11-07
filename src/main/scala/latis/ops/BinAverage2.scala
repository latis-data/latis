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
 * Assume 1D domain.
 * If range is tuple, take first element, for now.
 * Nested Functions not yet supported.
 */
class BinAverage2(binWidth: Double) extends Operation {
  //TODO: accept ISO 8601 time duration
  //TODO: support start value
  //TODO: use SampledData with DomainSet?
  //TODO: deal with 'length' metadata
  //TODO: deal with missing values
  
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
   */

  override def applyToFunction(f: Function): Option[Variable] = {
    val fit = PeekIterator(f.iterator)
    //If the function has no samples, return None for now.
    if (fit.isEmpty) None
    else {
      //get initial domain value so we know where to start
      //TODO: allow specification of initial value, e.g. so daily avg is midnight to midnight
      val startValue = getDomainValue(fit.peek)
      var nextValue = startValue
      var index = 0
      val domainMetadata = f.getDomain.getMetadata
      val rangeMetadata = reduce(f.getRange).getMetadata //first scalar

      //Make an iterator of new samples
      val sampleIterator = new PeekIterator[Sample] {
        def getNext = {
          nextValue += binWidth
          if (fit.isEmpty) null
          else {
            //accumulate the samples for this bin
            val binnedSamples = ListBuffer[Sample]()
            while (fit.hasNext && getDomainValue(fit.peek) < nextValue) binnedSamples += fit.next
            
            //create domain with bin center as its value, reuse original metadata
            //TODO: munge metadata
            val domainValue = nextValue - 0.5 * binWidth //bin center
            val domain = Real(domainMetadata, domainValue)
            
            //compute statistics on range values
            val range = computeStatistics(binnedSamples) match {
              case Some(range) => range
              case None => {
                //fill empty bin with NaNs
                //TODO: or call getNext if we want to skip empty bins
                val mean = Real(rangeMetadata, Double.NaN)
                val min  = Real(Metadata("min"), Double.NaN)
                val max  = Real(Metadata("max"), Double.NaN)
                Tuple(mean, min, max) //TODO: add metadata, consider model for bins
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
    case t: Time => t.getJavaTime.toDouble //TODO: only if type Text? user would need to know/use native units
    case Number(d) => d
    case _ => throw new Error("BinAverage supports only one dimensional numeric domains.")
  }
  
  private def computeStatistics(samples: Seq[Sample]): Option[Tuple] = {
    //Assume all Samples have the same type
    //TODO: pattern match on range type so we can eventually extend this
    //TODO: make Function from these samples then apply mean, min, max?
    //TODO: consider CF binning conventions
    //TODO: use count to weigh bins

    /*
     * TODO: remove missing values
     * NaNs don't behave with min...
     * use exclude_missing operation?
     * use fill value instead of NaN to fill empty bins
     * 
     */
    //val samples2 = samples.map()
    
    if (samples.isEmpty) None
    else {
      //process only the first scalar, for now
      val rangeTemplate = reduce(samples.head.range)
      
      //get data as a Map: name -> array of values
      val data = samplesToDoubleMap(samples)
      val values = data(rangeTemplate.getName)
      
      val meanValue = values.sum / values.length
      val mean = Real(rangeTemplate.getMetadata, meanValue)

      //if the original data was already binned (i.e. has min and max value) then use them.
      val minValue = data.get("min") match {
        case Some(ms) => ms.min
        case None => values.min
      }
      val min = Real(Metadata("min"), minValue)
      
      val maxValue = data.get("max") match {
        case Some(ms) => ms.max
        case None => values.max
      }
      val max = Real(Metadata("max"), maxValue)
      
      Some(Tuple(mean, min, max))
    }
  }
  
  //If the range is a tuple, just use the first element
  private def reduce(v: Variable): Scalar = v match {
    case s: Scalar => s
    case Tuple(vars) => reduce(vars.head)
    case _: Function => throw new Error("Can't perform a bin average over a nested Function.")
  }
  
  //TODO: move to DataUtils?
  private def samplesToDoubleMap(samples: Seq[Sample]): Map[String, Array[Double]] = {
    //make a dataset from these samples then op on it
    val ds = Dataset(Function(samples))
    ds.toDoubleMap
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
