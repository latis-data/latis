package latis.ops.filter

import scala.collection.mutable.ListBuffer
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.ops.Operation
import latis.ops.OperationFactory
import latis.time.Time
import latis.time.TimeScale
import latis.util.iterator.PeekIterator

/**
 * For datasets with one-dimensional time domains, given an optional time zone offset from GMT (in hours, e.g., -8.0 for PST),
 * for each day, keep only the sample whose time is closest to local noon (which is approximately solar noon).
 */
class DailyNearestToNoon(timeZoneOffset: Double = 0.0) extends Operation {
  //TODO: accept ISO 8601 time duration?
  //TODO: deal with 'length' metadata
  //TODO: deal with missing values
  
  var timeScale: TimeScale = null //Becomes the Time Scale of the first sample in the Dataset (via setGlobalTimeScale)
  var emptyRange: Variable = null //Becomes the Function's range with scalar values filled with their fill values
  
  /**
   * Hardcode the length of a day in ms.
   */
  def getDailyBinWidth: Double = 86400000
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val fit = PeekIterator(f.iterator)
    //If the function has no samples, make a new Function around this iterator (since we can't reuse the orig)
    if (fit.isEmpty) Some(Function(f, fit))
    else {
      setGlobalTimeScale(fit.peek)
      emptyRange = fillScalarValues(f.getRange)
      
      //Get initial domain value so we know where to start
      val firstSampleTime = getDomainValue(fit.peek)                       

      val sampleIterator = getIteratorOfBinnedSamples(fit, firstSampleTime)
      val sampleTemplate = sampleIterator.peek
      
      Some(Function(sampleTemplate.domain, sampleTemplate.range, sampleIterator))
    }
  }
  
  /*
   * Make an iterator of new samples in bins
   */
  private def getIteratorOfBinnedSamples(fit: PeekIterator[Sample], nextVal: Double): PeekIterator[Sample] = {
    var nextValue = nextVal
    new PeekIterator[Sample] {
      def getNext = {
        nextValue += getDailyBinWidth
        if (fit.isEmpty) null
        else {
          //accumulate the samples for this bin
          val binnedSamples = ListBuffer[Sample]()
          while (fit.hasNext && getDomainValue(fit.peek) < nextValue) binnedSamples += fit.next
            
          getNearestToNoonSample(binnedSamples, nextValue)
        }
      }
    }
  }
  
  /**
   * Given a collection of Samples for one day, return the sample whose time of day is nearest to noon.
   */
  private def getNearestToNoonSample(samples: ListBuffer[Sample], nextDayVal: Double): Sample = {
    if (samples.isEmpty) {
      if (timeScale != null) {
        val timeNum = Time(nextDayVal-getDailyBinWidth).convert(timeScale).getNumberData.doubleValue //start of bin for the current day 
        val time = Time(timeScale, timeNum) 
        Sample(time, emptyRange)
      } else {
        throw new RuntimeException("Time Scale could not be identified.")
      }
    }
    else {
      //Multiply the number of ms in an hour by noon minus the offset, 
      //then, in case the offset put us in a different day, mod that by the number of ms in a day
      val localNoonInUTC = ((getDailyBinWidth / 24.0) * (12.0 - timeZoneOffset)) % getDailyBinWidth //ms since midnight
      
      val distancesFromNoon = samples map {
        case Sample(t: Time, _) => {
          val timeOfDay = t.getJavaTime.toDouble % getDailyBinWidth //ms since midnight
          math.abs(timeOfDay - localNoonInUTC)
        }
        case _ => throw new RuntimeException("Domain variable must be Time.")
      }
      
      val indexOfNearest = distancesFromNoon.zipWithIndex.min._2 
      samples(indexOfNearest) 
    }
  }
  
  /**
   * Extract the value of the domain variable in the given sample as a Double.
   * Error if not a 1D numeric Scalar. Time will be treated as unix ms.
   */
  private def getDomainValue(sample: Sample): Double = sample.domain match {
    case t: Time => t.getJavaTime.toDouble
    case _ => throw new RuntimeException("DailyNearestToNoon supports only one-dimensional Time domains.")
  }
  
  /**
   * Given a sample, store the Time Scale of the domain globally (error if domain isn't Time).
   */
  private def setGlobalTimeScale(sample: Sample): Unit = sample.domain match {
    case t: Time => timeScale = t.getUnits
    case _ => throw new RuntimeException("DailyNearestToNoon supports only one-dimensional Time domains.")
  }
  
  /**
   * Recurse through a Variable to replace its Scalar values with their fill values, 
   * while maintaining the Variable's original structure.
   */
  private def fillScalarValues(variable: Variable): Variable = variable match {
    case scalar: Scalar => scalar.updatedValue(scalar.getFillValue.toString)
    case tuple: Tuple   => Tuple(tuple.getVariables.map(fillScalarValues(_)), tuple.getMetadata())
    case _: Function => throw new RuntimeException("DailyNearestToNoon operation does not support nested functions.")
  }
  
}


object DailyNearestToNoon extends OperationFactory {
  
  override def apply(args: Seq[String]): DailyNearestToNoon = {
    if (args.length > 1) throw new UnsupportedOperationException("The DailyNearestToNoon operation only accepts one argument.")
    try {
      DailyNearestToNoon(args.head.toDouble)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The DailyNearestToNoon operation's time zone offset argument must be numeric: " + e.getMessage)
    }
  }
  
  override def apply(): DailyNearestToNoon = new DailyNearestToNoon()
    
  def apply(timeZoneOffset: Double): DailyNearestToNoon = new DailyNearestToNoon(timeZoneOffset)
  
}
