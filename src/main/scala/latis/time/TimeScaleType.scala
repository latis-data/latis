package latis.time

object TimeScaleType extends Enumeration {
  type TimeScaleType = Value

  val NATIVE = Value("NATIVE")
  val UTC    = Value("UTC")
  val TAI    = Value("TAI")
}