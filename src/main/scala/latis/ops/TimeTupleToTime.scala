package latis.ops

import latis.dm._
import latis.time.Time
import latis.metadata.Metadata
import latis.data.value.StringValue

/**
 * If a Tuple named "time" containing Text elements for year, month, and day
 * is encountered in the Dataset, replace it with a Time variable.
 * TODO: generalize to combinations including time.
 */
class TimeTupleToTime extends Operation {
  
  /**
   * The default applyToTuple isn't applied to Function domains
   * so override applyToSample.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    for (domain <- applyToVariable(sample.domain); range <- applyToVariable(sample.range)) yield Sample(domain, range)
  }
  
  override def applyToTuple(tuple: Tuple): Option[Variable] = {
    tuple.getName match {
      case "time" => tuple match {
        case Tuple(vars) => {
          /*
           * TODO:
           * build format for pieces? of in tuple metadata?
           * Time as formatted Text or java time?
           * ++ignore vname and just use units/format
           */
//          //make Map of var name to string value
//          val vMap = vars.map(v => {
//            val vname = v.getName  //get variable name
//            val value = v match {case Text(s) => s}  //extract string value
//            (vname, value)
//          }).toMap
          
//          //build up time value and format
//          val value = new StringBuilder()
//          val format = new StringBuilder()
//          
//          //TODO: handle bad config and time options
//          vMap("year")
          
          //extract text values and join with space
          //TODO: join with delimiter, problem when we use regex
          val value = vars.map(_ match {case Text(s) => s}).mkString(" ")
          //build up format string
          val format = vars.map(_.getMetadata("units") match {
            case Some(units) => units
            case None => throw new Error("A time Tuple must have units defined for each element.")
          }).mkString(" ")
          
          //make the Time variable
          val metadata = Metadata(Map("name" -> "time", "units" -> format))
          val time = Time("text", metadata, StringValue(value))
          Some(time)
        }
      }
      case _ => Some(tuple)  //do nothing
    }
  }
}

object TimeTupleToTime extends OperationFactory {
  override def apply(): TimeTupleToTime = new TimeTupleToTime
  override def apply(args: Seq[String]): TimeTupleToTime = TimeTupleToTime()
}