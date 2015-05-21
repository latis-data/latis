package latis.time

/**
 * Although converting between scales of the same type does not call for a 
 * leap second adjustment, we do need to account for leap seconds that occur
 * between TAI time scale epochs since they are recorded in UTC time. The
 * adjustment will be made to the epoch offset.
 */
class TaiToTaiConverter(scale1: TimeScale, scale2: TimeScale) extends BasicTimeConverter(scale1, scale2) {
  
  override def convert(time: Double): Double = {
    val lsdif = LeapSecondUtil.getLeapSeconds(scale2.epoch) - LeapSecondUtil.getLeapSeconds(scale1.epoch)
    time * unitFactor - (epochOffset + lsdif)
  }

}