package latis.ops

import latis.dm._
import latis.ops.agg.Join
import latis.ops.agg.BasicJoin

class Collection extends Join {

//  override def apply(base: Dataset, layer: Dataset): Dataset = {
//    (base, layer) match {
//      case (ds1: Dataset, ds2: Dataset) if (ds2.isEmpty) => ds1
//      case (ds1: Dataset, ds2: Dataset) if (ds1.isEmpty) => ds2
//      case (Dataset(f1: Function), Dataset(f2: Function)) => 
//        Dataset(Tuple(f1, f2), base.getMetadata)
//      case (Dataset(Tuple(vs1)), Dataset(f2: Function)) =>
//        Dataset(Tuple(vs1 :+ f2), base.getMetadata)
//      case (Dataset(f1: Function), Dataset(Tuple(vs2))) =>
//        Dataset(Tuple(f1 +: vs2), base.getMetadata)
//      case (Dataset(Tuple(vs1)), Dataset(Tuple(vs2))) =>
//        Dataset(Tuple(vs1 ++ vs2), base.getMetadata)
//    }
//  }

  override def apply(base: Dataset, layer: Dataset): Dataset = {
    (base, layer) match {
      case (ds1: Dataset, ds2: Dataset) if (ds2.isEmpty) => ds1
      case (ds1: Dataset, ds2: Dataset) if (ds1.isEmpty) => ds2
      case (Dataset(t: Tuple), Dataset(f: Function)) => t.getMetadata.get("name") match {
        case Some(n) => Dataset(Tuple(t, f), base.getMetadata)
        case None => t match {
          case Tuple(vs) => Dataset(Tuple(vs :+ f), base.getMetadata)
        }
      }
      case (Dataset(v1), Dataset(v2)) => Dataset(Tuple(v1, v2), base.getMetadata)
    }
  }
}
object Collection {
  
  def apply(): Collection = new Collection()

  def apply(ds1: Dataset, ds2: Dataset): Dataset = Collection()(ds1, ds2)

  def apply(dss: Seq[Dataset]): Dataset = Collection()(dss)
}
