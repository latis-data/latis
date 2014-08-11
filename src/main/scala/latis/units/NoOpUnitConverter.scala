package latis.units

import latis.dm.Variable
import latis.dm.Scalar

/**
 * Return the value that was passed in.
 */
class NoOpUnitConverter(from: UnitOfMeasure, to: UnitOfMeasure) extends UnitConverter(from, to) {

  //TODO: assert that from == to
  
  override def convert(variable: Scalar): Scalar = variable
  
  def convert(value: Double): Double = value

}
