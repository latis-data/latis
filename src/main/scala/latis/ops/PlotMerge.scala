package latis.ops

import latis.dm._
import latis.ops.agg.BasicJoin
import latis.ops.resample.NearestNeighborResampling2D

class PlotMerge(ds1: Dataset, ds2: Dataset) extends BinaryOperation {
  //TODO: Refactor this so that we can use different interpolation strategies

  override def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    
    val f1 = ds1 match {
      case Dataset(f: Function) => f
    }
  /*
    val domainSet = f1.iterator.toArray.map( p => p match { 
      case Sample(t: Tuple, _) => {
        val d1 = t.getVariables(0).getNumberData.doubleValue
        val d2 = t.getVariables(1).getNumberData.doubleValue
        (d1, d2)
      }
    })
  */
    val domainSet = f1.iterator.toArray.map( p => p match {
      case Sample(t: Tuple, _) => t
    })
    val ds = new NearestNeighborResampling2D(domainSet)(ds2)
    //val rvals = domainSet.map(p => intrp(p._1, p._2))
    BasicJoin(ds1, ds)
  }
}
