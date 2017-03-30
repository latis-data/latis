package latis.ops.health

import com.typesafe.scalalogging.LazyLogging
import latis.ops.Operation
import latis.metadata.Metadata
import latis.dm._
import latis.reader.CatalogReader
import java.net.HttpURLConnection
import java.net.URL
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.InputStream
import latis.ops.OperationFactory
import scala.collection.mutable.ArrayBuffer

/* TODO: Finalize this description/name of the class itself
 * 
 * Given a catalog dataset ("following the DCAT ontology"?), describe 
 * whether the datasets in the catalog are "alive" or not.
 * This means checking to see if a dataset can return a first record. 
 */
class CatalogDatasetLiveness extends Operation with LazyLogging {
 
  /**
   * Apply this Operation to the given Dataset.
   * (Overriden to change the dataset's name)
   */
  override def apply(dataset: Dataset): Dataset = dataset match { //TODO: Is there an easier way to change ds's name?
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
        val samples = ArrayBuffer[Sample]() 
        
        it.foreach { s => s match {
          case Sample(Text(name), _) => 
            samples += Sample(Text(Metadata("ds_name"), name), 
                         Text(Metadata("alive"), urlIsAlive(name + ".txt?&first()").toString)) 
                         
          case _ => samples += Sample(Text(Metadata("ds_name"), "[INVALID RECORD]"), 
                                 Text(Metadata("alive"), "N/A"))              
          }   
        }
        Some(Function(samples)) 
      }
    }
  }
  
  /*
   * Given a dataset name, return true if that dataset serves a first record.
   * Else, false, and log the error.  
   */
  def urlIsAlive(name: String): Boolean = {
    val baseUrl = "http://lasp.colorado.edu/lisird3/latis/" //TODO: generalize this 
    
    val url = new URL(baseUrl + name)
    val con = url.openConnection.asInstanceOf[HttpURLConnection]
    logger.info("Testing url: " + url.toString)
    val is = {
      try {
        con.getInputStream
      }
      catch {
        case e: Exception => con.getErrorStream
      }
    }
    
    var br: BufferedReader = null
    
    try {
      br = new BufferedReader(new InputStreamReader(is))
      
      br.readLine match {
        case "" => logger.info("Empty data found for " + url); false
        case "LaTiS Error: {" => logger.info("Dataset access failed for " + url + " with LaTiS Error: " + br.readLine); false
        case _ => logger.info(url.toString + " passed"); true
      }
      
    }
    catch {
      case e: java.lang.Exception => logger.info("Dataset access failed for " + url + ". Unable to read from LaTiS: " + e.getMessage); false
    } 
    finally {
      try { is.close } catch {case _: Exception => }
      try { br.close } catch {case _: Exception => }
    }
  }
  
}

object CatalogDatasetLiveness extends OperationFactory {
  override def apply(): CatalogDatasetLiveness = new CatalogDatasetLiveness
}