package latis.ops.resample

trait NoInterpolation2D extends Interpolation2D {

  def interpolator(domain: Array[Array[Double]], range: Array[Double]) = (x: Double, y: Double) => {
    val xs = domain(0).distinct
    val ys = domain(1).distinct

    val xcoord = xs.indexOf(x) match {
      case -1 => None
      case v: Int => Some(v)
    }

    val ycoord = ys.indexOf(y) match {
      case -1 => None
      case v: Int => Some(v)
    }

    (xcoord, ycoord) match {
      case (Some(index1: Int), Some(index2: Int)) => {
        val index = index1*ys.length+index2
        Some(range(index))
      }
      case (_, _) => None
    }
  }
}
