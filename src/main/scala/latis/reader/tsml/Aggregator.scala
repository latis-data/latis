package latis.reader.tsml

import latis.dm.Dataset
import scala.collection.mutable.ArrayBuffer
import latis.dm.Variable

class Aggregator(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  override protected def makeDataset(): Dataset = {
    //Get child dataset nodes
    val dsnodes = (tsml.xml \ "dataset" \ "dataset")
    //Make a dataset for each
    val dss = for (node <- dsnodes) yield {
      val tsml = Tsml(node)
      println(tsml)
      val adapter = TsmlAdapter(tsml)
      
      adapter.dataset
    }
    
    aggregate(dss)
  }  
  
  def aggregate(datasets: Seq[Dataset]): Dataset = {
    //Get all the top level variables from all of the datasets
    val vars = datasets.foldLeft(ArrayBuffer[Variable]())(_ ++ _.variables)
      
    //Make metadata
    val md = makeMetadata(tsml.dataset) //TODO: provo
    
    //Make aggregated dataset
    Dataset(vars, md) 
  }

  def close {}
}