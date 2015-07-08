package latis.ops

import latis.units.UnitOfMeasure
import latis.dm.Dataset
import latis.dm.Scalar
import latis.units.UnitConverter
import latis.time.TimeScale
import latis.time.TimeConverter
import latis.time.Time
import latis.dm.Sample

/**
 * Convert all Time variables to a Time of type Text with the given format.
 * The resulting time scale is assumed to be UTC.
 */
class TimeFormatter(format: String) extends Operation {
  //TODO: consider implementing as a UnitConversion
  //TODO: apply to primary time variable only? otherwise may have diff units and we'd like to reuse the same converter
  //don't worry about optimizing with reusable converter, yet
  
  /**
   * Convert any Time variables to a Time of type Text with the desired format.
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = scalar match {
    case t: Time => {
      val formatted_time = t.format(format)
      val md = t.getMetadata + ("units" -> format) + ("type" -> "text") + ("length" -> format.length.toString)
      val time = Time(md, formatted_time)
      Some(time)
    }
    case _ => Some(scalar) //no-op
  }
   
  /**
   * Override to apply to both domain and range variables.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    for (d <- applyToVariable(sample.domain); r <- applyToVariable(sample.range)) yield Sample(d,r)
  }
}

object TimeFormatter extends OperationFactory {
  
  /**
   * Constructor used by OperationFactory.
   */
  override def apply(args: Seq[String]): TimeFormatter = {
    //TODO: error handling
    TimeFormatter(args.head)
  }
  
  /**
   * Construct a TimeFormatter with the given format string.
   * The format must be supported by Java's SimpleDataFormat.
   */
  def apply(format: String) = {
    new TimeFormatter(format)
  }
}