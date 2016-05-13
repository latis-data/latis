package latis.ops.filter

import latis.dm.Scalar
import latis.ops.OperationFactory

/*
 *  Removes all data points of a given name from the dataset 
 *  that differ more than 'maxDelta' in value from their preceding points.
 */
class MaxDeltaFilter(name: String, maxDelta: Double) extends Filter {
  
  var currentValue: Double = Double.NaN
  
  /*
   * Apply operation to a Scalar
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    if (currentValue.isNaN && scalar.hasName(name)) {
      currentValue = scalar.getNumberData.doubleValue
      Some(scalar) //Assumes the first data point is always good data, which may not always be true
    } else {
      if (scalar.hasName(name)) {
        val nextValue = scalar.getNumberData.doubleValue
        val delta = scala.math.abs(currentValue - nextValue)
    
        if (delta > maxDelta) {
          None         //Delta is greater than maxDelta, remove
        } else {
          currentValue = nextValue
          Some(scalar) //Acceptable delta, keep
        }
      } else Some(scalar) 
    }
  }  

}


object MaxDeltaFilter extends OperationFactory {
  def apply(name: String, maxDelta: Double): MaxDeltaFilter = new MaxDeltaFilter(name, maxDelta)
}
