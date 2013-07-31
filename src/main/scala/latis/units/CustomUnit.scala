package latis.units

/**
 * Support a custom UnitOfMeasure by providing a base unit of measure
 * and a multiplicative scale factor and additive offset.
 * TODO: clarify how scale and offset are to be applied, see NetCDF conventions
 */
class CustomUnit(baseUnit: String, scale: Double, offset: Double) extends 
  UnitOfMeasure(baseUnit + "_times_" + scale + "_plus_" + offset) {
  
}