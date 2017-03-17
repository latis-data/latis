package latis.ops.resample

import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Number
import latis.dm.Tuple
import latis.data.Data

trait NoInterpolation extends Interpolation { 
  
  def interpolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double] = {
    //TODO: assert xs.length == ys.length
    (d: Double) => xs.indexOf(d) match {
      case -1 => None
      case index: Int => Some(ys(index))
    }
  }
  
  /**
   * Since there is no interpolation, we can support any type of Variable.
   */
  override def interpolate(samples: Array[Sample], domain: Scalar): Option[Sample] = {
    //TODO: assumes scalar or flat tuple for range for the purpose of getting fill values
    
    //Get domain values from the samples
    val xs: Array[Double] = samples.map(s => s match {
      case Sample(Number(x), _) => x
    })
    
    //interpolate value
    val x: Double = domain match {case Number(x) => x}
    
    val range = xs.indexOf(x) match {
      case -1 => {
        //need fill values
        //use first sample as a template
        val temps = samples.head.range match {
          case s: Scalar => Seq(s)
          case Tuple(vars) => vars
        }
        val filledVars = temps.map(v => v match {
          case s: Scalar => s(Data(s.getFillValue))
          case _ => throw new UnsupportedOperationException("NoInterpolation supports only scalars and flat tuples of scalars in the range, for now.")
        })
    
        filledVars.length match {
          case 1 => filledVars.head
          case _ => Tuple(filledVars)
        }
      }
      case index: Int => samples(index)
    }
    
    Some(Sample(domain, range))
  }
}