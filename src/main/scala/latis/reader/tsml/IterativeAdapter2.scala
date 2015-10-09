package latis.reader.tsml

import latis.data.Data
import latis.dm.Function
import latis.reader.tsml.ml.Tsml
import latis.util.DataMapUtils

/**
 * Base class for Adapters for data sources that have 'record' semantics.
 */
abstract class IterativeAdapter2[R](tsml: Tsml) extends TsmlAdapter(tsml) {
  //R is the type of record
  
  def getRecordIterator: Iterator[R] 
  
  def parseRecord(record: R): Option[Map[String, Data]] 
        
  override def makeFunction(f: Function): Option[Function] = {
    val maps = getRecordIterator.flatMap(parseRecord(_))
    if (maps.isEmpty) None
    else Some(DataMapUtils.dataMapsToFunction(maps, f))
  }
  
}
