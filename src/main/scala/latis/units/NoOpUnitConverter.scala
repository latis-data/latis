package latis.units

import latis.dm.Variable

/**
 * Return the value that was passed in.
 */
class NoOpUnitConverter(from: UnitOfMeasure, to: UnitOfMeasure) extends UnitConverter(from, to) {

  //TODO: assert that from == to
  
  override def convert(variable: Variable): Variable = variable
  
  def convert(value: Double): Double = value

}
