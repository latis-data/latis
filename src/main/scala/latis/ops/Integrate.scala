package latis.ops

import latis.dm.Function
import latis.dm.Number
import latis.dm.Real
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.WrappedFunction

/**
 * Integrates the innermost function of a dataset using a trapezoidal Riemann sum. 
 * Start and end poins of integral can be specified or left empty to integrate over the entire function. 
 */
class Integrate(start: Double = Double.NaN, end: Double = Double.NaN) extends Operation {
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val fin = f.getSample.findFunction
    fin match {
      case Some(ff) => Some(WrappedFunction(f,this))
      case None => Some(Real(rSum(f)))
    }
  }
  
  def varToDouble(v: Variable): Double = v match {
    case n: Number => n.doubleValue
    case t: Tuple => t.getVariables.map(varToDouble(_)).head
    case _ => Double.NaN
  }
  
  def rSum(function: Function): Double = {
    val it = function.iterator
    var sum = 0.0
    var s1 = it.next
    while(it.hasNext) {
      val s2 = it.next
      val h1 = varToDouble(s1.domain)
      val h2 = varToDouble(s2.domain)
      val b1 = varToDouble(s1.range)
      val b2 = varToDouble(s2.range)
      if((h1 >= start || start.isNaN) && (h2 <= end || end.isNaN)) sum = sum + (b1 + b2)*(h2-h1)
      s1 = s2
    }
    sum/2
  }

}