package latis.reader.tsml

import scala.io.Source
import scala.collection._
import latis.reader.tsml.ml.Tsml
import latis.util.RegEx
import latis.data.Data
import scala.collection.mutable.ArrayBuffer
import latis.dm._
import latis.time.Time

class GranuleAsciiAdapter(tsml: Tsml) extends GranuleAdapter(tsml) with AsciiAdapterHelper {
  
  /**
   * Read all the data for the Dataset, convert to proper form of Data based on its type, 
   * and store in Map by variable name.
   */
  def readData: Map[String, Data] = {
    //read text data, parse, and store temporary string values by variable name
    val map = mutable.Map[String,ArrayBuffer[String]]()
    
    val it = getRecordIterator
    while (it.hasNext) {
      val record = it.next
      val vs = parseRecord(record)
      //skip bad records (empty Map)
      if (vs.nonEmpty) for (vname <- origScalarNames) map.get(vname) match {
        case Some(buffer) => buffer append vs(vname)
        case None => {
          val buffer = ArrayBuffer[String](vs(vname))
          map += (vname -> buffer)
        }
      }
    }
    
    //convert tmp string values to Data
    //TODO: factor out into reusable function/
    val dataMap = mutable.Map[String,Data]()
    
    for (scalar <- origScalars) {
      val name = scalar.getName
      val buffer = map(name)
      //TODO: if length = 0?
      val data = scalar match {
        //case _: Time => ???
        //TODO: no special treatment for Time needed?
        //TODO: catch parse error and fill (see IterativeAsciiAdapter)
        case _: Real    => Data(buffer.map(_.toDouble))
        case _: Integer => Data(buffer.map(_.toLong))
        case _: Binary  => ???
        case _: Text    => {
          //get length from tsml or longest string
          val length: Int = scalar.getMetadata("length") match {
            case Some(l) => l.toInt
            case None => Math.max(buffer.map(_.length).max, Text.DEFAULT_LENGTH) //at least as big as default=4
          }
          Data(buffer, length)
        }
      }
      
      dataMap += (name -> data)
    }

    dataMap
  }

}
