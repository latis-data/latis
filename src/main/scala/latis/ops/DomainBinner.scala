package latis.ops

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.util.StringUtils
import latis.util.iterator.MappingIterator
import latis.util.iterator.PeekIterator
import latis.metadata.Metadata

/**
 * Used for domains that represent a range of values but have only one explicit value.
 * The domain will be mapped to a Tuple with 'start_name' and 'end_name' Variables.
 * The parameter 'knownValues' indicates whether the known values for each bin are the
 * start or the end of the bin. The parameter 'fillVal' is used to fill in the 
 * unknown start/end value at the start/end of the function. 
 */
class DomainBinner(knownValues: String, fillVal: String) extends Operation {
  
  var (binStart, binStop): (Variable, Variable) = (null, null)
  var pit: PeekIterator[Sample] = null
  
  /**
   * Returns a Tuple containing the bounds of this Variable
   */
  override def applyToScalar(scalar: Scalar): Option[Tuple] = {
    val name = scalar.getName
    knownValues match {
      case "start" => {
        binStart = scalar.updatedMetadata("name" -> s"start_$name")
        binStop = pit.peek match {
          case null => scalar(StringUtils.parseStringValue(fillVal, scalar)).
            asInstanceOf[Scalar].updatedMetadata("name" -> s"stop_$name")
          case Sample(d: Scalar, r) => d.updatedMetadata("name" -> s"stop_$name")
        }
      }
      case "end" => {
        binStart = binStop.asInstanceOf[Scalar].updatedMetadata("name" -> s"start_$name")
        binStop = scalar.updatedMetadata("name" -> s"stop_$name")
      }
    }
    Some(Tuple(List(binStart, binStop), Metadata("bounds")))
  }
  
  override def applyToSample(sample: Sample): Option[Sample] = {
    applyToVariable(sample.domain) match {
      case Some(t:Tuple) => sample.range match{
        case Tuple(vars) => Some(Sample(sample.domain, Tuple(vars :+ t)))
        case other => Some(Sample(sample.domain, Tuple(other, t)))
      }
      case _ => None
    }
  }
  
  override def applyToFunction(function: Function): Option[Variable] = {
    val temp = function.getDomain
    val name = temp.getName
    pit = PeekIterator(function.iterator)
    binStop = temp(StringUtils.parseStringValue(fillVal, temp)).
      asInstanceOf[Scalar].updatedMetadata("name" -> s"stop_$name")
    
    super.applyToFunction(Function(function, pit))
  }

}

object DomainBinner extends OperationFactory {
  
  def apply(knownValue: String, fillVal: String): DomainBinner = new DomainBinner(knownValue, fillVal)
  
  override def apply(args: Seq[String]): DomainBinner = new DomainBinner(args(0), args(1))
  
}