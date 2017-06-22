package latis.units

class UnitOfMeasure(unit: String) {

  override def toString: String = unit
}

object UnitOfMeasure {
  
  def apply(unit: String): UnitOfMeasure = new UnitOfMeasure(unit)
}