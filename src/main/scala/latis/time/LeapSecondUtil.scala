package latis.time

import java.util.Date
import java.util.TreeMap
import scala.io.Source

object LeapSecondUtil {
  
  lazy val _origLeapSecondMap: TreeMap[Date, Double] = loadLeapSecondData
  
  /**
   * Gets the leap second file as TreeMap[Date, Double].
   */
  private def loadLeapSecondData: TreeMap[Date,Double]  = {
    val map = new TreeMap[Date,Double]

    val epoch: Date = new Date(Time.isoToJava("1900-01-01"))
    val etime: Long = epoch.getTime

    val src = getSource
    
    try {
      val lines = src.getLines.filterNot(_.startsWith("#"))

      lines.foreach(line => {
        val ss = line.takeWhile(_!='#').trim.split("\\s+") //cut comments after data
        val t = ss(0).toLong * 1000 + etime //convert to ms since 1970
        val date = new Date(t)
        val ls = ss(1).toDouble
        map.put(date, ls)
      })
        
    } finally {
      src.close
    }
    
    map
  }
  
  /**
   * Read the leap second source file.
   * It also has last update and expiration dates in it.
   * It's scale is seconds since 1900.
   */
  private def getSource: Source = {
    //TODO: check expiration date?
    //Source.fromURL("ftp://utcnist.colorado.edu/pub/leap-seconds.list") //unreliable
    Source.fromFile("src/main/scala/latis/time/leap-seconds.list")
  }
  
  /**
   * Return the number of leap seconds that have accumulated as of the given date.
   */
  def getLeapSeconds(date: Date): Double = _origLeapSecondMap.floorKey(date) match {
    case null => 10 //everything before 1972 has 10 leap seconds
    case key: Date => _origLeapSecondMap.get(key)
  }

}