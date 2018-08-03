package latis.util

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.metadata.ServerMetadata
import latis.ops.Operation
import latis.reader.CatalogReader
import latis.reader.DatasetAccessor

/**
 * Construct a Dataset of LaTiS capabilities:
 * Available datasets, output options, and filter options.
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
      val catalogDs = CatalogReader().getDataset().rename("name", "dataset_name")
      catalogDs match {
        case Dataset(Function(it)) => {
          Function(
            it.toSeq.map { 
              case Sample(name, _) => name 
            }
          )
        }
      }
    }
    
    val outputOptions: Function = {
      val md1: Metadata = Metadata().addName("output_option")
      val md2: Metadata = Metadata().addName("output_description") 
      Function(
        ServerMetadata.availableSuffixes.
        sortBy(suffixInfo => suffixInfo.suffix). //put in alphabetical order
        map(suffixInfo => (Tuple(Text(md1, suffixInfo.suffix), Text(md2, suffixInfo.description)))) 
      )
    }
    
    val filterOptions: Function = { 
      val md1: Metadata = Metadata().addName("operation_option")
      val md2: Metadata = Metadata().addName("operation_description")
      val md3: Metadata = Metadata().addName("operation_usage") 
      Function(  
        ServerMetadata.availableOperations.
        sortBy(opInfo => opInfo.name). //put in alphabetical order
        map(opInfo => (Tuple(Text(md1, opInfo.name), Text(md2, opInfo.description), Text(md3, opInfo.usage))))
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
