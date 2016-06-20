package latis.ops

import latis.dm._
import latis.ops.agg.BasicJoin
import latis.ops.resample.NearestNeighborResampling2D

class PlotMerge extends BinaryOperation {
  //TODO: Refactor this so that we can use different interpolation strategies

  override def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    
    val f1 = ds1 match {
      case Dataset(f: Function) => f
    }

    val domainSet = f1.iterator.toArray.map( p => p match {
      case Sample(t: Tuple, _) => t
    })
    val ds = new NearestNeighborResampling2D(domainSet)(ds2)
    //since BasicJoin doesnt like non-scalar domains,we will do it ourselves
    //this returns a dataset 
    //BasicJoin(ds1, ds)
    //
  
    val newRange1 = ds1 match {
      case Dataset(Function(it)) => it.toArray.map( p => p match {
        case Sample(_, s: Scalar) => s
      })
    }
    val newRange2 = ds match {
      case Dataset(Function(it)) => it.toArray.map( p => p match {
        case Sample(_, s: Scalar) => s
      })
    }
    val range = newRange1.zip(newRange2).map( p => Tuple(p._1, p._2) )
    val newData = domainSet.zip(range).map( p => Sample(p._1, p._2) ).toSeq
    Dataset(Function(newData))
  }
}

object PlotMerge {
  def apply(): PlotMerge = PlotMerge()
  def apply(ds1: Dataset, ds2: Dataset): Dataset = PlotMerge()(ds1, ds2)
}
