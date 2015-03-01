package latis.ops

import latis.units.UnitOfMeasure
import latis.dm.Dataset
import latis.dm.Scalar
import latis.units.UnitConverter
import latis.time.TimeScale
import latis.time.TimeConverter
import latis.time.Time
import latis.dm.Sample

/**
 * Currently works only for time unit conversion.
 */
class UnitConversion(variableName: String, unit: UnitOfMeasure) extends Operation {
  //TODO: specific variable by name vs all variables with a given unit
  //TODO: wrap UnitConverter logic into this
  
  private var converter: TimeConverter = null
  
  /**
   * Override to get the unit converter set up before delegating to super
   * to apply the operation to the variables in the dataset.
   */
  override def apply(dataset: Dataset) = {
    converter = dataset.unwrap.findVariableByName(variableName) match {
      case Some(v) => v match {
        case s: Scalar => s.getMetadata("units") match {
          case Some(u) => {
            //TODO: assuming Time, for now
            val origUnit = TimeScale(u)
            /*
             * TODO: this constructor creates a default time scale with java units (ms since 1970)
             * Since both formatted times have the same TimeScale, we get a NoOp converter.
             * Can TimeScale deal with diff formats?
             */
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

//TODO: not being applied to domain?
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    if (scalar.hasName(variableName)) {
      Some(converter.convert(scalar.asInstanceOf[Time])) //TODO: lost name, presumably all metedata
      //TODO: change units in metadata, responsibility of converter? should converter be an Operation? move it inside here?
    } else Some(scalar) //no-op
  }
   
  /**
   * Override to apply to both domain and range variables.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    for (d <- applyToVariable(sample.domain); r <- applyToVariable(sample.range)) yield Sample(d,r)
  }
}

object UnitConversion extends OperationFactory {
  
  /**
   * Constructor used by OperationFactory.
   */
  override def apply(args: Seq[String]): UnitConversion = {
    //TODO: error handling
    val vname = args.head
    val uom = TimeScale(args(1))
    new UnitConversion(vname, uom)
  }
  
  def apply(vname: String, unit: String) = {
    //TODO: assuming Time for now
    val uom = TimeScale(unit)
    new UnitConversion(vname, uom)
  }
}