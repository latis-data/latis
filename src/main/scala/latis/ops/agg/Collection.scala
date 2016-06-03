package latis.ops

import latis.dm._
import latis.ops.agg.Join

class Collection extends Join {

  override def apply(base: Dataset, layer: Dataset): Dataset = {
    (base, layer) match {
      case (ds1: Dataset, ds2: Dataset) if (ds2.isEmpty) => ds1
      case (ds1: Dataset, ds2: Dataset) if (ds1.isEmpty) => ds2
      case (Dataset(f1: Function), Dataset(f2: Function)) => 
        Dataset(Tuple(f1, f2), base.getMetadata)
      case (Dataset(Tuple(vs1)), Dataset(f2: Function)) =>
        Dataset(Tuple(vs1 :+ f2), base.getMetadata)
      case (Dataset(f1: Function), Dataset(Tuple(vs2))) =>
        Dataset(Tuple(f1 +: vs2), base.getMetadata)
      case (Dataset(Tuple(vs1)), Dataset(Tuple(vs2))) =>
        Dataset(Tuple(vs1 ++ vs2), base.getMetadata)
    }
  }
}

object Collection {
  
  def apply(): Collection = new Collection()

  def apply(ds1: Dataset, ds2: Dataset): Dataset = Collection()(ds1, ds2)

  def apply(dss: Seq[Dataset]): Dataset = Collection()(dss)
}
