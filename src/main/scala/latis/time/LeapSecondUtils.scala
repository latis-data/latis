package latis.time

import scala.collection.immutable.TreeMap
import java.util.Date
import latis.reader.tsml.TsmlReader

object LeapSecondUtils {

   def loadLeapSecondData: TreeMap[Date,Double] = {
     var reader: TsmlReader = null
     try {
       reader = TsmlReader("datasets/test/leap_seconds.tsml")
       val ds = reader.getDataset
       //TODO: put into TreeMap
       
     } finally {
       reader.close
     }
     
     ???
   }
}