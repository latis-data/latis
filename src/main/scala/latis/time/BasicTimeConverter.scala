package latis.time

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
    val t = convert(time.doubleValue)
    Time(t, scale2)
  }
}
