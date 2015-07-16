package latis.time

import latis.util.LatisProperties

object TimeScaleType extends Enumeration {
  type TimeScaleType = Value

  val NATIVE = Value("NATIVE")
  val UTC    = Value("UTC")
  val TAI    = Value("TAI")
  
  //Note, using def instead of lazy val to support tests.
  def default: TimeScaleType = LatisProperties.get("time.scale.type") match {
    case Some(s) => TimeScaleType.withName(s)  //TODO: handle error
    case None => NATIVE
  }
}