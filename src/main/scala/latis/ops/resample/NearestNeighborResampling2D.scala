package latis.ops.resample

import latis.dm._

class NearestNeighborResampling2D(domainSet: Iterable[Variable]) 
  extends Resampling(domainSet) with NearestNeighborInterpolation2D {
  
  override def interpolator(domain: Array[Array[Double]], range: Array[Double]) =
    super[NearestNeighborInterpolation2D].interpolator(domain, range)
  
}
