package latis.ops

import latis.units.UnitOfMeasure
import latis.dm.Dataset
import latis.dm.Scalar
import latis.units.UnitConverter
import latis.time.TimeScale
import latis.time.TimeConverter
import latis.time.Time

class UnitConversion(variableName: String, unit: UnitOfMeasure) extends Operation {
  //TODO: specific variable by name vs all variables with a given unit
  //TODO: wrap UnitConverter logic into this
  
  private var converter: TimeConverter = null
  
  /**
   * Override to get the unit converter set up before delegating to super
   * to apply the operation to the variables in the dataset.
   */
  override def apply(dataset: Dataset) = {
    converter = dataset.findVariableByName(variableName) match {
      case Some(v) => v match {
        case s: Scalar => s.getMetadata("units") match {
          case Some(u) => {
            //TODO: assuming Time, for now
            val origUnit = TimeScale(u)
            TimeConverter(origUnit, unit.asInstanceOf[TimeScale])
          }
          case None => throw new Error("UnitConversion: Variable has no units: " + variableName)
        }
        case _ => throw new Error("UnitConversion: Variable is not a Scalar: " + variableName)
      }
      case None => throw new Error("UnitConversion: Could not find variable: " + variableName)
    }
    
    super.apply(dataset)
  }

  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    if (variableName == scalar.getName) {
      Some(converter.convert(scalar.asInstanceOf[Time])) //TODO: lost name, presumably all metedata
      //TODO: change units in metadata, responsibility of converter? should converter be an Operation? move it inside here?
    } else Some(scalar) //no-op
  }
  
}

object UnitConversion {
  
  def apply(vname: String, unit: String) = {
    //TODO: assuming Time for now
    val uom = TimeScale(unit)
    new UnitConversion(vname, uom)
  }
}