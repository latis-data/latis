package latis.ops

import latis.dm.Function
import latis.dm.Number
import latis.dm.Real
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.WrappedFunction
import latis.time.Time

/**
 * Integrates the innermost function of a dataset using a trapezoidal Riemann sum. 
 * Start and end points of integral can be specified or left empty to integrate over the entire function. 
 */
abstract class Integration(a: Double = Double.NaN, b: Double = Double.NaN) extends Operation {
  
  /**
   * Integration is only applied to the innermost function
   */
  override def applyToFunction(f: Function): Option[Variable] = {
    val fin = f.getSample.findFunction
    fin match {
      case Some(ff) => Some(WrappedFunction(f,this))
      case None => Some(Real(integrate(subsection(f, a, b))))
    }
  }
  
  def integrate(f: Function): Double
  
  /**
   * Takes the samples from the function for which the domain is between start and stop.
   * NaN indicates that no values should be filtered at that bound.
   */
  def subsection(f: Function, start: Double, stop: Double): Function = {
    Function(f.getDomain, f.getRange, f.iterator.dropWhile(!start.isNaN && _.domain.getNumberData.doubleValue < start)
                                                .takeWhile(stop.isNaN || _.range.getNumberData.doubleValue <= stop), f.getMetadata)
  }
  
  /**
   * Get the Double value of a Number or Time, otherwise NaN.
   */
  def varToDouble(v: Variable): Double = v match {
    case Number(d) => d
    case t: Tuple => t.getVariables.map(varToDouble(_)).head //only takes the first value in a Tuple for now
    case _ => Double.NaN
  }

}
