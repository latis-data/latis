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
 * Construct a Dataset of LaTiS capabilities:
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
    val datasets: Function = {
      val dir = LatisProperties.getOrElse("dataset.dir", "src/main/resources/datasets") //TODO: confirm that this works in prod and dev
      Function(
        getListOfFiles(dir).map(f => {
        val n = f.getName
        val name = n.substring(0, n.lastIndexOf('.')) //drop the file extension
        val md: Metadata = Metadata().addName("dataset_name") 
        Text(md, name)
        })
      )
    }
    
    val outputOptions: Function = {
      val md1: Metadata = Metadata().addName("output_option")
      val md2: Metadata = Metadata().addName("output_description") //TODO: is there a cleaner way to name these scalars than using "md1", "md2"?
      Function(
        ServerMetadata.availableSuffixes.
        sortBy(suffixInfo => suffixInfo.suffix). //put in alphabetical order
        map(suffixInfo => (Tuple(Text(md1, suffixInfo.suffix), Text(md2, suffixInfo.description)))) //TODO: breaks if a value is null; I changed ServerMetadata to "getOrElse" "" instead of null. Was that okay?
      )
    }
    
    val filterOptions: Function = { 
      val md1: Metadata = Metadata().addName("operation_option")
      val md2: Metadata = Metadata().addName("operation_description")
      val md3: Metadata = Metadata().addName("operation_usage") //TODO: is there a cleaner way to name these scalars than using "md1", "md2", "md3"?
      Function(  
        ServerMetadata.availableOperations.
        sortBy(opInfo => opInfo.name). //put in alphabetical order
        map(opInfo => (Tuple(Text(md1, opInfo.name), Text(md2, opInfo.description), Text(md3, opInfo.usage)))) //TODO: breaks if a value is null; I changed ServerMetadata to "getOrElse" "" instead of null. Was that okay?
      )
    } 
    
    val capabilities: Tuple = Tuple(datasets, outputOptions, filterOptions)
    val md = Metadata().addName("latis_capabilities")
    val ds = Dataset(capabilities, md)
    ds
  }
  
  def close: Unit = {}
  
}


object LatisCapabilities {
  def apply(): LatisCapabilities = new LatisCapabilities
}
