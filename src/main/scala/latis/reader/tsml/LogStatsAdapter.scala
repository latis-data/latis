package latis.reader.tsml

import scala.collection.mutable.ArrayBuffer

import latis.data.value.DoubleValue
import latis.data.value.StringValue
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Text
import latis.ops.Operation
import latis.ops.filter.Selection
import latis.reader.JsonReader
import latis.reader.tsml.ml.Tsml
import latis.time.Time
import latis.util.PeekIterator
import latis.util.StringUtils

class LogStatsAdapter(tsml: Tsml) extends IterativeAdapter[(Sample, Sample)](tsml) {
  
  def close = {}
  
  val ops = ArrayBuffer[Operation](Selection("level!=WARN"))//warnings never start or end a request
  lazy val log = JsonReader(getUrl).getDataset(ops)
  var buf = ArrayBuffer[Sample]() //keep a buffer of log entries that have been read but not paired
  var lit: Iterator[Sample] = null
  
  override def handleOperation(op: Operation): Boolean = op match {
    case s: Selection => getOrigScalarNames.contains(s.vname) match {
      case true => {ops += s; true}
      case false => false
    }
    case _ => super.handleOperation(op)
  }
  
  def getRecordIterator = {
    lit = log match {case Dataset(f: Function) => f.iterator}
    new PeekIterator[(Sample, Sample)] {
      def getNext = {
        nextRequest match {
          case Some(s) => findEnd(s) match {
            case Some(e) => (s, e)
            case None => null
          }
          case None => null
        }
      }
    }
  }
  
  def parseRecord(rec: (Sample, Sample)) = {
    val (s1, s2) = rec
    val vars = getOrigScalars
    
    val time = s1.domain.asInstanceOf[Time].toIso
    val duration = s2.domain.asInstanceOf[Time].getJavaTime - s1.domain.asInstanceOf[Time].getJavaTime
    val request = s1.findVariableByName("message").get.asInstanceOf[Text].stringValue.drop("Processing request: ".length)
    val result = s2.findVariableByName("message").get.asInstanceOf[Text].stringValue //"Request complete" or error
    
    Some(Map(
      vars(0).getName -> StringValue(StringUtils.padOrTruncate(time, vars(0))),
      vars(1).getName -> DoubleValue(duration/1000),
      vars(2).getName -> StringValue(StringUtils.padOrTruncate(request,vars(2))),
      vars(3).getName -> StringValue(StringUtils.padOrTruncate(result, vars(3)))))
  }
  
  /**
   * Determine if the given Sample has a Text Variable named 'name' with a value that matches pattern.
   */
  def hasStringMatch(s: Sample)(name: String, pattern: String) = s.findVariableByName(name) match {
    case Some(Text(s)) => s.contains(pattern)
    case _ => false
  }
  
  /**
   * Gets the next log entry that indicates the start of a request.
   */
  def nextRequest: Option[Sample] = {
    buf.find(hasStringMatch(_)("message","Processing request: ")) match {
      case Some(s) => {
        buf -= s
        Some(s)
      }
      case None => {
        val (pre, suf) = lit.span(! hasStringMatch(_)("message","Processing request: "))
        buf ++= pre
        lit = suf
        if(lit.hasNext) Some(lit.next) else None
      }
    }
  }
  
  /**
   * Finds the next log entry in the same thread as the entry 'sample'.
   * Because warnings are filtered from the log, this will be either the "Request complete" 
   * notification or the error that ended the request. 
   */
  def findEnd(sample: Sample): Option[Sample] = {
    val thread = sample.findVariableByName("thread").get.asInstanceOf[Text].stringValue
    buf.find(hasStringMatch(_)("thread",thread)) match {
      case Some(s) => {
        buf -= s
        Some(s)
      }
      case None => {
        val (pre, suf) = lit.span(! hasStringMatch(_)("thread",thread))
        buf ++= pre
        lit = suf
        if(lit.hasNext) Some(lit.next) else None
      }
    }
  }

}