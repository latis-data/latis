package latis.ops.resample

import latis.dm._

class NearestNeighborResampling(domainSet: Iterable[Variable]) 
  extends Resampling(domainSet) with NearestNeighborInterpolation {
  
  override def interpolator(xs: Array[Double], ys: Array[Double]) = 
    super[NearestNeighborInterpolation].interpolator(xs, ys)
  
}