package latis.ops

import latis.dm._

class RiemannTrapezoidIntegration(start: Double = Double.NaN, stop: Double = Double.NaN) extends Integration(start,stop) {
  
  /**
   * Performs a trapezoidal Riemann sum over a function
   */
  def integrate(function: Function): Double = {
    val it = function.iterator
    var sum = 0.0
    var s1 = it.next
    while(it.hasNext) {
      val s2 = it.next
      val h1 = varToDouble(s1.domain)
      val h2 = varToDouble(s2.domain)
      val b1 = varToDouble(s1.range)
      val b2 = varToDouble(s2.range)
      sum = sum + (b1 + b2)*(h2-h1)
      s1 = s2
    }
    sum/2
  }

}

object RiemannTrapezoidIntegration extends OperationFactory {
  
  override def apply(args: Seq[String]): RiemannTrapezoidIntegration = {
    if (args.length > 2) throw new UnsupportedOperationException("RiemannTrapezoidIntegration accepts up to 2 arguments")
    try {
      RiemannTrapezoidIntegration(args(0).toDouble, args(1).toDouble)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("Integration requires two or zero numeric arguments")
    }
  }
  
  def apply(start: Double, stop: Double): RiemannTrapezoidIntegration = new RiemannTrapezoidIntegration(start, stop)
  
  override def apply(): RiemannTrapezoidIntegration = new RiemannTrapezoidIntegration()
  
}