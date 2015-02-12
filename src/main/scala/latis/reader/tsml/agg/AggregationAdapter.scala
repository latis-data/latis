package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.ml.Tsml

/**
 * Base class for Adapters that aggregate (combine) Datasets.
 */
abstract class AggregationAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Given a Dataset that contains other Datasets
   * apply the aggregation logic to make a single Dataset.
   */
  def aggregate(dataset: Dataset): Dataset
  
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
      adapter.getDataset
    }
    
    collect(dss)
  }  
  
  override protected def makeDataset(ds: Dataset): Dataset = aggregate(ds)
    
    
  /**
   * Make a single Dataset that contains the Datasets to be aggregated.
   */
  def collect(datasets: Seq[Dataset]): Dataset = {
    //Get all the top level variables from all of the datasets
    //val vars = datasets.foldLeft(ArrayBuffer[Variable]())(_ ++ _.variables)
      
    //Make metadata
    val md = makeMetadata(tsml.dataset) //TODO: provo
    
    //Make aggregated dataset
    Dataset(datasets, md) 
  }

  //TODO: close all contributing adapters
  def close = {}
}