package latis.time

import latis.util.LatisProperties

object TimeScaleType extends Enumeration {
  type TimeScaleType = Value

  val NATIVE = Value("NATIVE")
  val UTC    = Value("UTC")
  val TAI    = Value("TAI")
  
  lazy val default: TimeScaleType = LatisProperties.get("time.scale.type") match {
    case Some(s) => TimeScaleType.withName(s)  //TODO: handle error
    case None => NATIVE
  }
}