package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.dm.implicits._
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.ml.Tsml
import scala.collection.mutable.ArrayBuffer
import latis.dm.Tuple

/**
 * Base class for Adapters that aggregate (combine) Datasets.
 */
abstract class AggregationAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Keep a list of adapters so we can close them.
   */
  private val adapters = ArrayBuffer[TsmlAdapter]()
  
  /**
   * Given a Dataset that contains other Datasets (now as a tuple)
   * apply the aggregation logic to make a single Dataset.
   */
  def aggregate(left: Dataset, right: Dataset): Dataset
  
  /**
   * Combine each aggregate Dataset into a single Dataset.
   * These are simply grouped together, no merging logic applied.
   */
  override protected def makeOrigDataset: Dataset = {
    //TODO: consider deeper nesting of dataset nodes
    //Get child dataset nodes
    val dsnodes = (tsml.xml \ "dataset" \ "dataset")
    //Make a dataset for each
    val dss = for (node <- dsnodes) yield {
      val tsml = Tsml(node)
      val adapter = TsmlAdapter(tsml)
      adapters += adapter
      adapter.getDataset
    }
    
    collect(dss)
  }  
  
  override protected def makeDataset(ds: Dataset): Dataset = ds match {
    case Dataset(Tuple(vars)) => Dataset(vars.reduceLeft(aggregate(_,_).unwrap), ds.getMetadata)
  }
    
    
  /**
   * Make a single Dataset that contains the Datasets to be aggregated.
   */
  def collect(datasets: Seq[Dataset]): Dataset = {
    //Get all the top level variables from all of the datasets
    //val vars = datasets.foldLeft(ArrayBuffer[Variable]())(_ ++ _.variables)
      
    //Make metadata
    val md = makeMetadata(tsml.dataset) //TODO: provo
    
    //Make aggregated dataset
    //TODO: do we need a Collection type?
    //TODO: use a fold with a binary agg op?
    //for now, replace datasets with a tuple
    val vars = datasets.map(_.unwrap)
    val tup = Tuple(vars)
    Dataset(tup, md) 
  }

  /**
   * Close all contributing adapters.
   */
  def close = adapters.foreach(_.close)
}
