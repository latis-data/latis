package latis.util

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.metadata.ServerMetadata
import latis.ops.Operation
import latis.reader.DatasetAccessor
import latis.util.FileUtils.getListOfFiles

/**
 * Return a Dataset of LaTiS capabilities:
 * Datasets served, output options, and filter options.
 */
class LatisCapabilities extends DatasetAccessor {
  
  override def getDataset: Dataset = getCapabilities
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    val dataset = getCapabilities
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }
  
  /**
   * Return the Dataset of LaTiS capabilities,
   * pulled from the "datasets" directory and the latis.properties file.
   */
  def getCapabilities: Dataset = {
    val datasets: List[Text] = {
      val dir = LatisProperties.getOrElse("dataset.dir", "src/main/resources/datasets") //TODO: confirm how to make this work in prod and dev
      getListOfFiles(dir).map(f => {
        val n = f.getName
        val name = n.substring(0, n.lastIndexOf('.')) //drop the file extension
        val md: Metadata = Metadata().addName("dataset_name")
        Text(md, name)
      })
    }
    
    val outputOptions: List[Tuple] = { 
      ServerMetadata.availableSuffixes.
        sortBy(suffixInfo => suffixInfo.suffix). //put in alphabetical order
        map(suffixInfo => (Tuple(Text(suffixInfo.suffix), Text(suffixInfo.description)))). //TODO: breaks if value is null; temporarily changed ServerMetadata to "getOrElse" "" instead of null
        toList
    }
    
    val filterOptions: List[(Tuple)] = { 
      ServerMetadata.availableOperations.
        sortBy(opInfo => opInfo.name). //put in alphabetical order
        map(opInfo => (Tuple(Text(opInfo.name), Text(opInfo.description), Text(opInfo.usage)))). //TODO: breaks if value is null; temporarily changed ServerMetadata to "getOrElse" "" instead of null
        toList
    } 
    
    //TODO: build Functions in the anonymous functions, not here
    val datasetsFunc: Function = Function(datasets)
    val outputFunc: Function = Function(outputOptions)
    val filterFunc: Function = Function(filterOptions)
    
    val capabilitiesTup: Tuple = Tuple(datasetsFunc, outputFunc, filterFunc)
    val md = Metadata().addName("latis_capabilities")
    val ds = Dataset(capabilitiesTup, md)
    
//    println("Datasets: " + datasets)
//    println("Output Options: " + outputOptions)
//    println("Filter Options: " + filterOptions)
   
    ds
  }
  
  def close: Unit = {}
  
}


object LatisCapabilities {
  def apply(): LatisCapabilities = new LatisCapabilities
}
