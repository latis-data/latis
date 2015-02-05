package latis.time

import latis.units.UnitConverter

abstract class TimeConverter(scale1: TimeScale, scale2: TimeScale) extends UnitConverter(scale1, scale2) {
  
  def convert(time: Time): Time
  
}

object TimeConverter {
  
  def apply(scale1: TimeScale, scale2: TimeScale): TimeConverter = {
    if (scale1 == scale2) NoOpTimeConverter(scale1, scale2)
    else (scale1.tsType, scale2.tsType) match {
      //TODO: use time scale type to deal with leap seconds
      //case (TAI, UTC) => new TaiToUtcConverter(scale1, scale2)
      //case (UTC, TAI) => new UtcToTaiConverter(scale1, scale2)
      //case (TAI, TAI) if (scale1.epoch != scale2.epoch) => new TaiToTaiConverter(scale1, scale2)
      case _ => new BasicTimeConverter(scale1, scale2)
    }
  }
}