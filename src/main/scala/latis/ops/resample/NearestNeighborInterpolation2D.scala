package latis.ops.resample

/**
 * Interpolates a value to whichever given value it is closest to. 
 * If equidistant to two points, it will return the lower point. 
 */
trait NearestNeighborInterpolation2D extends Interpolation2D {
  
  def interpolator(domain: Array[Array[Double]], range: Array[Double]) = (x: Double, y: Double) => {
    val xs = domain(0).distinct
    val ys = domain(1).distinct

    val xcoord = xs.find( _ > x) match {
      case Some(upperBound) => {
        //some upper bound to the x value exists
        //so find a lower bound
        val i = xs.indexOf(upperBound)
        if(i == 0) i
        else {
          val lowerBound = xs(i-1)
          if((x-lowerBound) <= (upperBound-x)) (i-1)
          else i
        }
      }
      case None => {
        //no upper bound to the x value exists
        //use last x vlaue as lower bound
        xs.length-1
      }
    }

    val ycoord = ys.find( _ > y) match {
      case Some(upperBound) => {
        //some upper bound to the y value exists
        //find lower bound
        val i = ys.indexOf(upperBound)
        if(i == 0) i
        else {
          val lowerBound = ys(i-1)
          if((y-lowerBound) <= (upperBound-y)) (i-1)
          else i
        }
      }
      case None => {
        //no upper bound to the y value exists
        //use last y value as lower bound
        ys.length-1
      }
    }

    //hooray index magic/math!
    val index = xcoord*ys.length+ycoord
    if (index < range.length) Some(range(index))
    else None
  }
}
