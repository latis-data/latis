package latis.units

class UnitOfMeasure(unit: String) {

  override def toString = unit
}

object UnitOfMeasure {
  
  def apply(unit: String) = new UnitOfMeasure(unit)
}