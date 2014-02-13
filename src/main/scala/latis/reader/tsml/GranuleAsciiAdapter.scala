package latis.reader.tsml

import scala.io.Source
import scala.collection._
import latis.reader.tsml.ml.Tsml

class GranuleAsciiAdapter(tsml: Tsml) extends GranuleAdapter(tsml) {
  

  
//  //suck in entire granule, for now
//  def readData: immutable.Map[Name, immutable.Seq[Value]] = {
//    val map = initDataMap
//        
//    val it = recordIterator
//    while (it.hasNext) {
//      val record = it.next
//      val vs = parseRecord(record)
//      //skip bad records (empty Map)
//      if (vs.nonEmpty) for (vname <- origVariableNames) map(vname) append vs(vname)
//    }
//    
//    immutableDataMap(map)
//  }
  
}