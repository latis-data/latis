package latis.ops.filter

import com.typesafe.scalalogging.LazyLogging
import scala.math._
import latis.dm._
import latis.metadata._
import latis.ops._

/*
 *  Removes all data points from the dataset that differ more than 'maxDelta' in value from their preceding points
 */
class MaxDeltaFilter(val maxDelta: Double) extends Filter {
  
  var currentValue: Double = Double.NaN
  var nextValue:    Double = Double.NaN
  
  /*
   * Apply operation to a Function.
   * Assumes Function is of type: t -> v
   */
  override def applyToFunction(function: Function): Option[Function] = {
    function match {
      case Function(it) => {
        val s = it.next
        val v = s.range match {
          //set currentValue to value of function's first Scalar
          case v: Variable => currentValue = v.getNumberData.doubleValue 
        }
      }
    }
    
    function.iterator.foreach(applyToSample(_))
    
//    val it = function.iterator
//    while (!it.isEmpty) {
//      nextValue = it.next.range.getNumberData.doubleValue
//      //val compareBool = scala.math.abs(currentValue - nextValue) > maxDelta
//      currentValue = nextValue
//    }
    
    Some(Function(function.getDomain, function.getRange, function.iterator)) //This is possibly a good thing to do
  }
  
  /*
   * Apply operation to a Sample
   * Assumes Sample is of type: t -> v
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    //applyToVariable(sample.range)
    val x = sample.getVariables.drop(1).map(applyToVariable(_)) //Should apply to only the single range Variable of sample
    x.find(_.isEmpty) match {
      case Some(_) => None //found a delta exceeding maxDelta, exclude the entire sample
      case None    => Some(Sample(x(0).get, x(1).get))                                
    }
  }
  
//  /*
//   * Apply operation to a Variable, depending on type
//   */
//  override def applyToVariable(variable: Variable): Option[Variable] = variable match {
//    case scalar: Scalar     => applyToScalar(scalar)
//    case sample: Sample     => applyToSample(sample)
//    //case tuple: Tuple       => applyToTuple(tuple)
//    case function: Function => applyToFunction(function)
//  }
  
  /*
   * Apply operation to a Scalar
   */
  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
//    val nextValue = scalar.getNumberData.doubleValue
//    val delta = scala.math.abs(currentValue - nextValue)
//    
//    if (delta > maxDelta) {
//      None         //Delta is greater than maxDelta, remove
//      //TODO: continuously consider the following Scalars
//    } else {
//      Some(scalar) //Acceptable delta, keep
//      //TODO: set currentValue to this nextValue?
//    }     
    None
  }
  
  
  
}

object MaxDeltaFilter extends OperationFactory {
  def apply(maxDelta: Double): MaxDeltaFilter = new MaxDeltaFilter(maxDelta)
}
