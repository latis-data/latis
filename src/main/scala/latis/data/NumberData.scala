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
//just encourages instantiation of value class
//object NumberData {
//  def unapply(num: NumberData) = Some(num.doubleValue) //always expose double? use subclass if we want other type
//}
