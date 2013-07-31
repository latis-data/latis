package latis.units

import latis.dm._
import latis.metadata.Metadata

abstract class UnitConverter(from: UnitOfMeasure, to: UnitOfMeasure) {

  /*
   * TODO:
   * support dimensional analysis: time, length, mass,...
   * unit converter for mass would only convert variables (within a function...) that have mass type units
   */
  
  /**
   * Abstract method. Override to do the conversion.
   */
  def convert(value: Double): Double 
  
  //TODO: operate on complex Variables
  def convert(variable: Variable): Variable = variable match {
    case Number(d) => {
      //TOD: copy metadata changing units
      //val md = ScalarMd(variable.metadata.properties + (("units", to.toString())))
      Real(convert(d)) //TODO: use builder to get type right
    } 
    case _ => throw new UnsupportedOperationException("There is no unit conversion support for this Variable.")
  }
  
}

object UnitConverter {
  
  def apply(from: UnitOfMeasure, to: UnitOfMeasure) = {
    if (from == to) new NoOpUnitConverter(from, to)  //TODO: override equals?
    else throw new UnsupportedOperationException("There is no unit conversion support from " + from + " to " + to)
  }
}