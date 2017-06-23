package latis.reader.tsml

import latis.dm._
import latis.time._
import latis.reader.tsml.ml.Tsml
import latis.util.StringUtils
import latis.data.Data

class FileListGenerator(tsml: Tsml) extends IterativeAdapter2[Int](tsml) {
  
  def close: Unit = {}
  
  val (start, stop, stride) = this.getProperty("range") match {
    case Some(s) => s.split(';') match {
      case Array(start, stop) => (start.toInt, stop.toInt, 1)
      case Array(start, stop, stride) => (start.toInt, stop.toInt, stride.toInt)
    }
    case None => throw new RuntimeException("FileListGenerator requires a 'range' attribute.")
  }
  
  def getRecordIterator: Iterator[Int] = {
    Iterator.range(start, stop, stride)
  }
  
  def parseRecord(rec: Int): Option[Map[String, Data]] = {
    val names = getOrigScalarNames
    //each scalar should have a "pattern" attribute that tells how to format the variable
    val formats = names.map(tsml.getVariableAttribute(_, "pattern"))
    //make a string for each variable from its "pattern" property. 
    //the record is interpreted as an iso string. 
    val strings = formats.map(_.format(rec, Time.isoToJava(rec.toString)))
    
    val datas = (strings zip getOrigScalars).map(p => StringUtils.parseStringValue(p._1, p._2))
    Some((names zip datas).toMap)
  }

}