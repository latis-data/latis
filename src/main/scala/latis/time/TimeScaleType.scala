package latis.time

object TimeScaleType extends Enumeration { //}("NATIVE", "UTC", "TAI") {
  type TimeScaleType = Value
  //val NATIVE, UTC, TAI = Value
  val NATIVE = Value("NATIVE")
  val UTC    = Value("UTC")
  val TAI    = Value("TAI")
}