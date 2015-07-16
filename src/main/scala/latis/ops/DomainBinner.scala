package latis.ops

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.util.StringUtils
import latis.util.iterator.MappingIterator
import latis.util.iterator.PeekIterator

/**
 * Used for domains that represent a range of values but have only one explicit value.
 * The domain will be mapped to a Tuple with 'start_name' and 'end_name' Variables.
 * The parameter 'knownValues' indicates whether the known values for each bin are the
 * start or the end of the bin. The parameter 'fillVal' is used to fill in the 
 * unknown start/end value at the start/end of the function. 
 */
class DomainBinner(knownValues: String, fillVal: String) extends Operation {
  
  var (binStart, binEnd): (Variable, Variable) = (null, null)
  var pit: PeekIterator[Sample] = null
  
  override def applyToScalar(scalar: Scalar) = {
    val name = scalar.getName
    knownValues match {
      case "start" => {
        binStart = scalar.updatedMetadata("name" -> s"start_$name")
        binEnd = pit.peek match {
          case null => scalar(StringUtils.parseStringValue(fillVal, scalar)).
            asInstanceOf[Scalar].updatedMetadata("name" -> s"end_$name")
          case Sample(d: Scalar, r) => d.updatedMetadata("name" -> s"end_$name")
        }
      }
      case "end" => {
        binStart = binEnd.asInstanceOf[Scalar].updatedMetadata("name" -> s"start_$name")
        binEnd = scalar.updatedMetadata("name" -> s"end_$name")
      }
    }
    Some(Tuple(binStart, binEnd))
  }
  
  override def applyToSample(sample: Sample) = {
    applyToVariable(sample.domain) match {
      case Some(d) => Some(Sample(d, sample.range))
      case None => None
    }
  }
  
  override def applyToFunction(function: Function) = {
    val temp = function.getDomain
    val name = temp.getName
    pit = PeekIterator(function.iterator)
    binEnd = temp(StringUtils.parseStringValue(fillVal, temp)).
      asInstanceOf[Scalar].updatedMetadata("name" -> s"end_$name")
    
    super.applyToFunction(Function(function, pit))
  }

}

object DomainBinner extends OperationFactory {
  
  def apply(knownValue: String, fillVal: String) = new DomainBinner(knownValue, fillVal)
  
  override def apply(args: Seq[String]) = new DomainBinner(args(0), args(1))
  
}