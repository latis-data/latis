package latis.time

class UtcToTaiConverter(scale1: TimeScale, scale2: TimeScale) extends LeapSecondConverter(scale1, scale2) {
  
  /**
   * Convert the given time on the original TimeScale to the new TimeScale.
   * Delegate to the superclass to do the basic conversion then apply the
   * leap second adjustment.
   */
  override def convert(time: Double): Double =  {
    //convert then account for leap seconds
    super.convert(time) + getLeapSecondAdjustment(time)
  }
  
  /**
   * Get the number of accumulated leap seconds at the new scale's epoch.
   */
  protected def getLeapSecondsAtNewEpoch: Double =  {
    LeapSecondUtil.getLeapSeconds(scale2.epoch)
  }

}