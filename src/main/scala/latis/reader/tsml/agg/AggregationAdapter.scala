package latis.reader.tsml.agg

import latis.dm.Dataset
import scala.collection.mutable.ArrayBuffer
import latis.dm.Variable
import latis.reader.tsml.ml.Tsml
import latis.reader.tsml.TsmlAdapter

abstract class AggregationAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  //TODO: use metadata for agg'd dataset from tsml
  //  should we just make a single Dataset containing datasets?
  //  aggregation will look for all child datasets
  //  more consistent with other Operations
  //  keep things in the Dataset monad
  
  def aggregate(dataset: Dataset): Dataset
  
  override protected def makeDataset(): Dataset = {
    //TODO: consider deeper nesting of dataset nodes
    //Get child dataset nodes
    val dsnodes = (tsml.xml \ "dataset" \ "dataset")
    //Make a dataset for each
    val dss = for (node <- dsnodes) yield {
      val tsml = Tsml(node)
      //println(tsml)
      val adapter = TsmlAdapter(tsml)
      adapter.dataset
    }
    
    val ds = collect(dss)
    aggregate(ds)
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
    Dataset(datasets, md) 
  }

  def close {}
}