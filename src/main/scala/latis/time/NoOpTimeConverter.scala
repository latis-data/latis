package latis.time

/**
 * Return the value that was passed in.
 */
class NoOpTimeConverter(from: TimeScale, to: TimeScale) extends TimeConverter(from, to) {

  //TODO: assert that from == to
  
  def convert(time: Time): Time = time
  
  def convert(value: Double): Double = value

}
