package latis.ops

import latis.dm.Dataset

trait Aggregation {

  def apply(datasets: Seq[Dataset]): Dataset
}

object Aggregation {
  
  def apply(aggType: String): Aggregation = aggType match {
    case "basic" => ???
    case "union" => ???
    case "join" => ???  //intersect?
  }
}