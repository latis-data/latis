package latis.reader.tsml.agg

import latis.dm.Dataset
import latis.ops.Idempotence
import latis.ops.Operation
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.ml.Tsml
import latis.ops.filter.Filter

/**
 * Base class for Adapters that aggregate (combine) Datasets.
 */
abstract class AggregationAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Keep a list of adapters so we can close them.
   */
  protected val adapters = (tsml.xml \ "dataset").map(n => TsmlAdapter(Tsml(n)))
  
  /**
   * Given a Dataset that contains other Datasets (now as a tuple)
   * apply the aggregation logic to make a single Dataset.
   */
  def aggregate(left: Dataset, right: Dataset): Dataset
  
  
  override def getDataset(ops: Seq[Operation]) = {
    val (filter, others) = ops.partition(_.isInstanceOf[Filter])
    val dss = adapters.map(_.getDataset(filter))
    
    val ds = collect(dss)
    
    ops.foldLeft(ds)((dataset, op) => op(dataset)) //doesn't handle any Operations
  }
  
  override protected def makeDataset(ds: Dataset): Dataset = {
    getDataset(List())
  }
    
    
  /**
   * Make a single Dataset that contains the Datasets to be aggregated.
   */
  def collect(datasets: Seq[Dataset]): Dataset = {
      
    //Make metadata
    val md = makeMetadata(tsml.dataset) //TODO: provo
    
    //Apply the aggregation to the datasets
    Dataset(datasets.reduceLeft(aggregate(_,_)).unwrap, md) 
  }

  /**
   * Close all contributing adapters.
   */
  def close = adapters.foreach(_.close)
}
