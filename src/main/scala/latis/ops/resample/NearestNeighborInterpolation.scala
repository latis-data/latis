package latis.ops.resample

/**
 * Interpolates a value to whichever given value it is closest to.
 * If equidistant to two points, it will return the lower point.
 */
trait NearestNeighborInterpolation extends Interpolation {

  def interpolator(xs: Array[Double], ys: Array[Double]): Double => Option[Double] = (x: Double) => {
    xs.find(_ > x) match { //find the upper bound
      case Some(ub) => {
        val i = xs.indexOf(ub)
        if (i == 0) Some(ys.head) //less than all x, use first y
        else {
          val lb = xs(i - 1)
          if((x - lb) <= (ub - x)) Some(ys(i - 1)) //closer to lower bound (or in exact middle)
          else Some(ys(i)) //closer to upper bound
        }
      }
      case None => Some(ys.last) //greater than all x, use last y
    }
  }

}
