package latis.time

import latis.metadata.Metadata
import latis.dm.Number
import latis.dm.Text

class BasicTimeConverter(scale1: TimeScale, scale2: TimeScale) extends TimeConverter(scale1, scale2) {

  // Get time zero for each time scale in seconds since 1970
  private val etime1 = scale1.epoch.getTime().toDouble / 1000.0
  private val etime2 = scale2.epoch.getTime().toDouble / 1000.0
  
  // Get the unit (step size) for each time scale in seconds
  private val unit1 = scale1.unit.seconds
  private val unit2 = scale2.unit.seconds
  
  // Compute the offset between scale start times in units of the resulting time scale
  protected val epochOffset = (etime2 - etime1) / unit2
  
  // Compute the multiplication factor between scale units
  protected val unitFactor = unit1 / unit2
  
  def convert(time: Double): Double = {
    time * unitFactor - epochOffset
  }
  
  def convert(time: Time): Time = {
    //TODO: assert time.scale == scale1
    //TODO: if Integer preserve long
    
    //Time as Text will have a java TimeScale so use getJavaTime
    val t1 = time match {
      case _: Text => time.getJavaTime.toDouble
      case Number(d) => d
    }
    //val t = convert(time.getNumberData.doubleValue)
    val t = convert(t1)
    
    //redefine units in metadata
    val md = Metadata(time.getMetadata.getProperties + ("units" -> scale2.toString)) //TODO: make sure TimeScale(scale.toString) works
    //Time(scale2, t)
    Time(md, t)
  }
}
