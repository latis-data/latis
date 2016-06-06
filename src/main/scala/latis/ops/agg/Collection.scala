package latis.ops.agg

import latis.dm._

class Collection extends Join {

  override def apply(base: Dataset, layer: Dataset): Dataset = {
    (base, layer) match {
      case (ds1: Dataset, ds2: Dataset) if (ds2.isEmpty) => ds1
      case (ds1: Dataset, ds2: Dataset) if (ds1.isEmpty) => ds2
      case (Dataset(t @ Tuple(tv)), Dataset(f: Function)) => t.getName match {
        case "unknown" => Dataset(Tuple(tv :+ f), base.getMetadata)
        case _ => Dataset(Tuple(Seq(t, f)), base.getMetadata)
      }
      case (Dataset(f: Function), Dataset(t @ Tuple(tv))) => t.getName match {
        case "unknown" => Dataset(Tuple(f +: tv), base.getMetadata)
        case _ => Dataset(Tuple(Seq(f, t)), base.getMetadata)
      }
      case (Dataset(t1 @ Tuple(tv1)), Dataset(t2 @ Tuple(tv2))) => (t1.getName, t2.getName) match {
        // If neither variable is "named", then we can go ahead and combine both of them
        case ("unknown", "unknown") => Dataset(Tuple(tv1 ++ tv2), base.getMetadata)
        // If one or the other is a named variable, we shouldn't combine
        case _ => Dataset(Tuple(t1, t2), base.getMetadata)
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
