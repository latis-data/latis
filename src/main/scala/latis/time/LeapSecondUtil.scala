package latis.time

import java.util.Date
import java.util.TreeMap
import scala.io.Source
import latis.reader.tsml.TsmlReader
import latis.dm.Dataset

object LeapSecondUtil {
  
  lazy val _origLeapSecondMap: TreeMap[Date, Double] = loadLeapSecondData
  
  /**
   * Gets the leap second file as TreeMap[Date, Double].
   */
  private def loadLeapSecondData: TreeMap[Date,Double]  = {
    val map = new TreeMap[Date,Double]

    val epoch: Date = new Date(Time.isoToJava("1900-01-01"))
    val etime: Long = epoch.getTime

    val lsds = readLeapSecondData
    
    val it = lsds.findFunction.get.iterator

    it.foreach(sample => {
      val t = sample.domain.asInstanceOf[Time].getJavaTime//ms since 1970
      val date = new Date(t)
      val ls = sample.range.getNumberData.doubleValue
      map.put(date, ls)
    })
    
    map
  }
  
  /**
   * Read the leap second source file.
   * It also has last update and expiration dates in it.
   * It's scale is seconds since 1900.
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