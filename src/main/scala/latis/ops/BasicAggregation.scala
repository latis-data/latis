package latis.ops

import latis.dm.Dataset
import scala.collection.mutable.ArrayBuffer
import latis.dm.Variable

class BasicAggregation extends Aggregation {

  /*
   * TODO:
   * could this be an operation on one dataset?
   * otherwise can't specify metadata
   * would we have to allow a dataset to contain a dataset? collection?
   *   probably
   * or just aggregate top level vars? keep ds namespace to avoid conflicts
   * support only 2 at a time? probably, joinLeft vs joinRight?
   * 
   * do like binary math: construct with primary, apply to other
   * but still issue of getting metadata, construct with that too?
   *   Aggregation(ds: Dataset, md: Metadata) ?
   * 
   * look into relational algebra
   */
  def apply(datasets: Seq[Dataset]): Dataset = {
    //Get all the top level variables from all of the datasets
    //TODO: deal with name conflicts
    //  keep dataset name
//    val vars = datasets.foldLeft(ArrayBuffer[Variable]())(_ ++ _.variables)
//      
//    //Make metadata
//    val md = makeMetadata(tsml.dataset) //TODO: provo
//    
//    //Make aggregated dataset
//    Dataset(vars, md) 
    ???
  }
}