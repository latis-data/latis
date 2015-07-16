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
    
    val etime = TimeScale("seconds since 1900-01-01").epoch.getTime //TODO: should these be UTC seconds...

    it.foreach(_ match {case Sample(t: Time, Number(ls)) => map.put(new Date(t.getNumberData.longValue * 1000 + etime), ls)})
    
    map
  }
  
  /**
   * Read the leap second source file into a Dataset.
   */
  private def readLeapSecondData: Dataset = {
    var reader: TsmlReader = null
     try {
       reader = TsmlReader("leap_seconds.tsml")
       reader.getDataset.force
     } finally {
       if(reader != null) reader.close
     }
  }
  
  /**
   * Return the number of leap seconds that have accumulated as of the given date.
   * By our definition, everything before the UTC epoch (1972-01-01) has 0 leap seconds.
   * Note, some APIs model historical leap seconds (with varying length seconds) from 1961-1972.
   * This API is self-consistent but comparisons to others during this time range (or with epochs
   * in this time range) may vary by as much as 10 seconds.
   */
  def getLeapSeconds(date: Date): Double = _origLeapSecondMap.floorKey(date) match {
    case null => 0
    case key: Date => _origLeapSecondMap.get(key)
  }

}