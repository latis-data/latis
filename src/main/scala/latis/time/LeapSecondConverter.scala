package latis.time

import java.util.Date
import java.util.TreeMap

import scala.collection.JavaConversions.mapAsScalaMap

abstract class LeapSecondConverter(scale1: TimeScale, scale2: TimeScale) extends BasicTimeConverter(scale1, scale2) {
  
  private lazy val _leapSecondMap = makeLeapSecondMap
  
  /**
   * Get the number of accumulated leap seconds at the new scale's epoch.
   */
  protected def getLeapSecondsAtNewEpoch: Double
  
  /**
   * Put the leap second data (time stamps and amount of leap second 
   * adjustments) in a map that is optimized for the specific pair of
   * TimeScale-s represented by this TimeConverter.
   * The keys will be the leap second time stamps on the original scale.
   * The values will be the number of leap seconds since the new epoch.
   * This is a TreeMap so that the leap second adjustment for a given
   * input time can be readily acquired using floorKey(time).
   */
  private def makeLeapSecondMap: TreeMap[Double, Double] = {
    val map = new TreeMap[Double,Double]
        
    //Get the number of accumulated leap seconds at the new scale's epoch.
    val leapSecondsAtNewEpoch: Double = getLeapSecondsAtNewEpoch
    
    //Convert leap second timestamps to the original time scale to use as keys.
    val ts: TimeScale = TimeScale(TimeScale.JAVA, TimeScaleType.UTC)
    val conv: TimeConverter = TimeConverter(ts, scale1)
    
    val lsmap: scala.collection.mutable.Map[Date,Double] = mapAsScalaMap(LeapSecondUtil._origLeapSecondMap)
    lsmap.foreach(p => {
        val date: Date = p._1   //UTC Date just after a leap second adjustment was imposed
        var ls: Double = p._2 //accumulated leap second adjustment at that date
        
        //put leap seconds since the new epoch into the new scale's units
        ls = (ls - leapSecondsAtNewEpoch) / scale2.unit.seconds
        
        //convert the leap second time stamp to the original scale.
        val time: Double = date.getTime
        val t: Double = conv.convert(time)
        
        //put the leap second into the map
        map.put(t, ls)
    })
    
    map
  }
  
  /**
   * Return the number of leap seconds needed to adjust between
   * this TimeConverter's TimeScale-s. 
   */
  protected def getLeapSecondAdjustment(time: Double): Double = try {
    _leapSecondMap.get(_leapSecondMap.floorKey(time))
  } catch {case e: NullPointerException => 0}
  
  

}