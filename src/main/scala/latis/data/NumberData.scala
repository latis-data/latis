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
  
  def toDouble: Double = doubleValue
  //def toLong: Long = doubleValue.toLong //truncates
  def toLong: Long = Math.round(toDouble) //rounds
  
  //TODO: do we need ordering?
//  def compare(that: Number)(implicit ord: DoubleOrdering) = {
//    ord.compare(toDouble, that.toDouble)
//  }
  
  //TODO: do we need equality tests?
//  /**
//   * Number equality is based on the Double values.
//   */
//  override def equals(that: Any): Boolean = {
//    that.isInstanceOf[Number] && that.asInstanceOf[Number].toDouble == toDouble
//  }
//  
//  /**
//   * Override to be consistent with equals.
//   */
//  override def hashCode() = toDouble.hashCode()
}

/**
 * Object for pattern matching and exposing the data value as a Double.
 */
object NumberData {
  def unapply(num: NumberData) = Some(num.toDouble) //always expose double? use subclass if we want other type
}