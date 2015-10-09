package latis.ops

import org.apache.commons.math3.analysis.interpolation.SplineInterpolator

import latis.dm.Function

/**
 * 4th degree Newton-Cotes integration. Mimics IDL int_tabulate
 */
class NewtonCotesIntegration(start: Double = Double.NaN, stop: Double = Double.NaN) extends Integration(start, stop) {
  
  def integrate(f: Function): Double = {
    val (u,v) = f.iterator.duplicate
    val x = u.map(sam => varToDouble(sam.domain)).toArray 
    val y = v.map(sam => varToDouble(sam.range)).toArray
    val interp = new SplineInterpolator().interpolate(x,y) //use interpolation from apache commons math. 
    
    var segments = x.length - 1
    while(segments % 4 != 0) segments = segments + 1 //domain must be divided into equal length segments that are divisible by 4
    
    val h = (x.last - x.head)/segments //distance between each x value
    val x2 = (0 until segments + 1).map(_ * h + x.head) //equally spaced x values to evaluate the interpolation
    val y2 = x2.map(interp.value(_)) //interpolated y values at equally spaced x values. 
    
    var sum = 0.0
    for(j <- (0 until segments/4).map(_ * 4)) //loop through total number of segments, grouped into fours. 
      sum = sum + 7 * (y2(j + 4) + y2(j)) + 32 * (y2(j + 3) + y2(j + 1)) + 12 * (y2(j + 2)) //sum according to Boole's rule
    
    
    sum * 2 * h / 45
  }
  

}

object NewtonCotesIntegration extends OperationFactory {
  
  override def apply(args: Seq[String]): NewtonCotesIntegration = {
    if (args.length > 2) throw new UnsupportedOperationException("NewtonCotesIntegration accepts up to 2 arguments")
    try {
      NewtonCotesIntegration(args(0).toDouble, args(1).toDouble)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("Integration requires two or zero numeric arguments")
    }
  }
  
  def apply(start: Double, stop: Double): NewtonCotesIntegration = new NewtonCotesIntegration(start, stop)
  
  override def apply(): NewtonCotesIntegration = new NewtonCotesIntegration()
  
}