package latis.data

/**
 * Trait for Data that can provide a numerical value.
 */
trait NumberData extends Any with Data {
  
  def intValue: Int
  def longValue: Long
  def floatValue: Float
  def doubleValue: Double
  
  //def longValue: Long = doubleValue.toLong //truncates
  //def longValue: Long = Math.round(toDouble) //rounds
    
}

/**
 * Object for pattern matching and exposing the data value as a Double.
 */
object NumberData {
  def unapply(num: NumberData): Option[Double] = Some(num.doubleValue)
}
