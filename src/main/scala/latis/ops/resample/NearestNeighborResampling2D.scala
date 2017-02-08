package latis.ops.resample

import latis.data.Data
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Number
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.implicits.variableToDataset
import latis.ops.Operation
import latis.ops.Split
import latis.ops.agg.BasicJoin
import latis.time.Time

class NearestNeighborResampling2D(domainSet: Iterable[Variable]) 
  extends Resampling2D(domainSet) with NearestNeighborInterpolation2D {

  override def interpolator(domain: Array[Array[Double]], range: Array[Double]) =
    super[NearestNeighborInterpolation2D].interpolator(domain, range)

}
