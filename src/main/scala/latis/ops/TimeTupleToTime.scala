package latis.ops

import latis.dm._
import latis.time.Time
import latis.metadata.Metadata
import latis.data.value.StringValue

/**
 * If a Tuple named "time" containing Text elements for date and time elements
 * is encountered in the Dataset, replace it with a Time Scalar of type Text.
 * Each component must have a "units" metadata property that adheres to the
 * Java SimpleDateFormat. The Text string values will be joined with " " to
 * make a single time value and the units will be joined with " " to create
 * a single units/format. These will be used to construct a new Time of type Text.
 */
class TimeTupleToTime extends Operation {
  
  /**
   * The default applyToTuple isn't applied to Function domains
   * so override applyToSample to handle both domain and range.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    for (domain <- applyToVariable(sample.domain); range <- applyToVariable(sample.range)) yield Sample(domain, range)
  }
  
  /**
   * If the Tuple has the name "time", combine the string values and units
   * to make a new Time of type Text. Otherwise, check the members of the Tuple.
   */
  override def applyToTuple(tuple: Tuple): Option[Variable] = tuple match {
    case Tuple(vars) if tuple.getName == "time" =>
      //extract text values and join with space
      //TODO: join with delimiter, problem when we use regex
      val value = vars.map(_ match {case Text(s) => s}).mkString(" ")
      //build up format string
      val format = vars.map(_.getMetadata("units") match {
        case Some(units) => units
        case None => throw new RuntimeException("A time Tuple must have units defined for each element.")
      }).mkString(" ")

      //make the Time variable
      val metadata = Metadata(Map("name" -> "time", "units" -> format))
      val time = Time("text", metadata, StringValue(value))
      Some(time)
    case Tuple(vars) =>
      Option(Tuple(vars.flatMap(applyToVariable(_))))
  }
}

object TimeTupleToTime extends OperationFactory {
  //Implement OperationFactory so we can invoke this as a processing instruction.
  override def apply(): TimeTupleToTime = new TimeTupleToTime
  override def apply(args: Seq[String]): TimeTupleToTime = TimeTupleToTime()
}
