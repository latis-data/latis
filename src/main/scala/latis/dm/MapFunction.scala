package latis.dm

import latis.dm.{ Variable => V }
import latis.metadata._
import scala.collection.SortedMap
import latis.util.iterator.PeekIterator

/**
 * Experiment with holding Function data in a SortedMap.
 *   Text -> Text only, for now
 */
//class MapFunction[D <: V, C <: V](domain: D, range: C, metadata: Metadata = EmptyMetadata, map: SortedMap[D,C]) 
class MapFunction(map: SortedMap[String, String]) extends SampledFunction(Text(""), Text("")) {

  override def apply(v: Variable): Option[Variable] = v match {
    case Text(s) => map.get(s).map(Text(_))
  }

  override def iterator = PeekIterator(map.iterator.map(p => Sample(Text(p._1), Text(p._2))))
}

object MapFunction {

  def apply(ds: Dataset): MapFunction = {
    val pairs = ds match {
      case Dataset(Function(it)) => it.map(s => s match {
        case Sample(Text(k), Text(v)) => k -> v
      }).toSeq
    }
    
    new MapFunction(SortedMap(pairs: _*))
  }
}