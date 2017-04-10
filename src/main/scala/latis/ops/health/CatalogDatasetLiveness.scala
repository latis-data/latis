package latis.ops.health

import scala.collection.mutable.ArrayBuffer
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Text
import latis.metadata.Metadata
import latis.ops.Operation
import latis.ops.OperationFactory
import latis.ops.filter.FirstFilter
import latis.reader.DatasetAccessor
import com.typesafe.scalalogging.LazyLogging

/* 
 * Given a catalog dataset following the DCAT ontology, describe 
 * whether the datasets in the catalog are "alive" or not.
 * This means checking to see if a dataset can return a first record. 
 */
class CatalogDatasetLiveness extends Operation with LazyLogging {
 
  /**
   * Apply this Operation to the given Dataset.
   * (Overriden to change the dataset's name)
   */
  override def apply(dataset: Dataset): Dataset = dataset match { 
    case Dataset(variable) => {
      val md = dataset.getMetadata
      applyToVariable(variable) match {
        case Some(v) => Dataset(v, md+("name", "dataset_health")) 
        case None => Dataset.empty
      }
    }
    case _ => dataset
  }
  
  /*
   * Determine whether the datasets listed in 
   * this catalog are "alive" or not.
   */
  override def applyToFunction(function: Function) = { 
    function match {
      case Function(it) => {
        val samples = it.map { s => s match {
          case Sample(Text(name), _) => 
            Sample(Text(Metadata("ds_name"), name), 
              Text(Metadata("alive"), dsIsAlive(name).toString)) 
                         
          case _ => Sample(Text(Metadata("ds_name"), "[INVALID RECORD]"), 
                      Text(Metadata("alive"), "N/A"))              
          }   
        }
        Some(Function(samples.toSeq)) 
      }
    }
  }
  
  /*
   * Given a dataset name, return true if that dataset serves a first record.
   * Else, false.  
   */
  def dsIsAlive(name: String): Boolean = {
    try {
      val ds = DatasetAccessor.fromName(name).getDataset(Seq(FirstFilter()))
      ds match {
        case Dataset(Function(it)) => it.hasNext
        case _ => false
      }
    } 
    catch {
      case e: Throwable => 
        logger.info("Dataset access failed for " + name + ". Unable to read from LaTiS: " + e.getMessage); false
    } 
  }
  
}

object CatalogDatasetLiveness extends OperationFactory {
  override def apply(): CatalogDatasetLiveness = new CatalogDatasetLiveness
}