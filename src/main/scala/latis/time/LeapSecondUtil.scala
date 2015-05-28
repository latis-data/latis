package latis.time

import java.util.Date
import java.util.TreeMap
import latis.dm.Dataset
import latis.dm.Function
import latis.reader.tsml.TsmlReader
import latis.dm.Sample
import latis.dm.Number

object LeapSecondUtil {
  
  lazy val _origLeapSecondMap: TreeMap[Date, Double] = loadLeapSecondData
  
  /**
   * Gets the leap second file as TreeMap[Date, Double].
   */
  private def loadLeapSecondData: TreeMap[Date,Double]  = {
    val map = new TreeMap[Date,Double]

    val lsds = readLeapSecondData
    
    val it = lsds match {case Dataset(f: Function) => f.iterator}

    it.foreach(_ match {case Sample(t: Time, Number(ls)) => map.put(new Date(t.getJavaTime), ls)})
    
    map
  }
  
  /**
   * Read the leap second source file into a Dataset.
   */
  private def readLeapSecondData: Dataset = {
    var reader: TsmlReader = null
     try {
       reader = TsmlReader("src/test/resources/datasets/test/leap_seconds.tsml")
       reader.getDataset.force
     } finally {
       if(reader != null) reader.close
     }
  }
  
  /**
   * Return the number of leap seconds that have accumulated as of the given date.
   */
  def getLeapSeconds(date: Date): Double = _origLeapSecondMap.floorKey(date) match {
    case null => 10 //everything before 1972 has 10 leap seconds
    case key: Date => _origLeapSecondMap.get(key)
  }

}