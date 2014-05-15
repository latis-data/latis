package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import biweekly.component.VEvent
import latis.data.Data
import biweekly.Biweekly
import scala.collection.mutable
import scala.collection.Map
import java.io.InputStream

class ICalendarAdapter(tsml: Tsml) extends IterativeAdapter[VEvent](tsml) {

  private var input: InputStream = null
  
  def getRecordIterator: Iterator[VEvent] = {
    //assume only one calendar
    input = getUrl.openStream
    val ical = Biweekly.parse(input).first
    scala.collection.JavaConversions.asScalaIterator(ical.getEvents.iterator)
  }

  def parseRecord(event: VEvent): Option[Map[String,Data]] = {
    val map = mutable.Map[String,Data]()
    map += ("dtstart" -> Data(event.getDateStart.getValue.getTime)) //TODO: time zone
    map += ("dtend" -> Data(event.getDateEnd.getValue.getTime)) //TODO: time zone
    map += ("summary" -> Data(event.getSummary.getValue))
    
    Some(map)
  }
  
  def close {
    input.close
  }
}