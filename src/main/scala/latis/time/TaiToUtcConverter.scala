package latis.time

class TaiToUtcConverter(scale1: TimeScale, scale2: TimeScale) extends LeapSecondConverter(scale1, scale2) {
  
  /**
   * Convert the given time on the original TimeScale to the new TimeScale.
   * Delegate to the superclass to do the basic conversion then apply the
   * leap second adjustment.
   */
  override def convert(time: Double): Double = {
    //convert, then account for leap seconds
    val t: Double = super.convert(time) - getLeapSecondAdjustment(time)
        
    //If the given time is during a leap second, the UTC time should be 
    //paused at the beginning of that second.
    //Because the leap second does not exist in a UTC scale, the end of the pre-leap second
    //should lead directly to the second following the leap second.
    //Don't confuse the fact that a UTC scale is continuous but that a UTC clock effectively pauses to match reality.
    if (isLeapSecond(time)) {
      val unit: Double = scale2.unit.seconds
      math.floor(t * unit) / unit
    } else t
  }
    
  /**
   * Is the given time (on the TAI scale) during a leap second.
   * If the leap second adjustment is different one second later, then it is a leap second.
   */
  def isLeapSecond(time: Double): Boolean = {
    val adj1: Double = getLeapSecondAdjustment(time)
    val adj2: Double = getLeapSecondAdjustment(time + 1.0/scale2.unit.seconds)
    return (adj1 != adj2)
  }
  
  /**
   * Get the number of accumulated leap seconds at the new scale's epoch.
   * Because we are converting from a TAI scale that counts leap seconds,
   * we need to account for the leap seconds that occur between the epochs
   * of the two scales.
   */
  protected def getLeapSecondsAtNewEpoch: Double = {
    //Using the original scale may look wrong, but that's the way it works out
    //when we put back the leap seconds between the epochs.
    LeapSecondUtil.getLeapSeconds(scale1.epoch)
  }
}
