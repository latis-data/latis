package latis.reader

import scala.collection.mutable.ArrayBuffer
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Text
import latis.ops.Operation
import latis.util.FileUtils
import latis.util.LatisProperties
import latis.metadata.Metadata

class CatalogGenerator extends DatasetAccessor {  
  
  private lazy val datasetNames: List[String] = getListOfDatasets   
  private lazy val catalogDataset: Dataset = generateCatalogDataset
  
  /**
   * Return the Dataset that this accessor is responsible for (catalogDataset).
   */
  override def getDataset(): Dataset = catalogDataset
  
  /**
   * Return the catalogDataset with the given Operations applied to it.
   */
  override def getDataset(ops: Seq[Operation]): Dataset = ops.reverse.foldRight(getDataset)(_(_))
  
  override def close = {}
  
  //---- Helper Functions ------------------------------------------------------------------------
  
  /*
   * Produce a list of the names of all datasets that live in 
   * the "dataset.dir" as defined in latis.properties.
   */
  def getListOfDatasets: List[String] = {
    val dsdir: String = LatisProperties.getOrElse("dataset.dir", "datasets")
    val fileList = FileUtils.getListOfFiles(dsdir) 
    val fileNames = ArrayBuffer[String]()
    
    for (x <- fileList) {
      fileNames += x.toString.substring(dsdir.length()+1, x.toString.length-5) //trim leading file path and ".tsml"
    }
    
    fileNames.toList
  }
  
  def getDatasetNames = datasetNames
  
  /*
   * Convert a list of dataset names into
   * a dataset of the form i -> ds_name
   */
  def generateCatalogDataset = {
    val samples = datasetNames.map(n => {
      Sample(Index(), Text(Metadata("ds_name"), n))
    })
    Dataset(Function(samples))
  }
  
}