package latis.reader.tsml

import java.util.Date
import latis.reader.tsml.ml.Tsml
import latis.time.Time
import latis.util.FileUtils
import latis.ops.DomainBinner


class LogListAdapter(tsml: Tsml) extends FileListAdapter(tsml) {
  
  /**
   * Override to place current log at end of list.
   */
  override def getRecordIterator = {
    val dir = getUrl.getPath 
    val files = FileUtils.listAllFilesWithSize(dir)
    
    val (t, f) = files.partition(file => regex.findFirstMatchIn(file) match {
      case Some(m) => m.group(1) == null
      case None => false
    })
    f ++ t iterator
  }
  
  /**
   * Override to add current time if there is no time match.
   */
  override def extractValues(record: String) = {
    val fileName = record.split(',')(0)
    val size = record.split(',')(1)
    regex.findFirstMatchIn(fileName) match {
      case Some(m) => {
        if(m.group(1) != null) (m.subgroups :+ fileName) :+ size //add the file name
        else List(Time(new Date()).format("yyyy-MM-dd"), fileName, size)
      }
      case None => List[String]()
    }
  }
  
  override def piOps = {
    val dombin = tsml.getProcessingInstructions("domBin").map(s => DomainBinner(s.split(',')))
    super.piOps ++ dombin
  }

}