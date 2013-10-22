package latis.data

/**
 * Trait for Data that can provide a numerical value.
 */
trait NumberData extends Any with Data {
  /*
   * TODO: NumberData? NumericData?
   * trait NumericData extends Data with NumberData 
   * is this useful? not if only for scalars
   * but do we want to say that a byte buffer is all numbers? probably not
   * always just one number?
   * 
   * assume a single number, for now, use for numeric value classes
   * 
   * with ScalaNumericAnyConversions
   */
  
  def intValue: Int
  def longValue: Long
  def floatValue: Float
  def doubleValue: Double
  
  //def toLong: Long = doubleValue.toLong //truncates
  //def toLong: Long = Math.round(toDouble) //rounds
    
}

/**
 * Object for pattern matching and exposing the data value as a Double.
 */
object NumberData {
  def unapply(num: NumberData) = Some(num.doubleValue)
}
