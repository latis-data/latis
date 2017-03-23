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

/* TODO: Finalize this description/name of the class itself
 * 
 * Given a catalog dataset (following the DCAT ontology?), describe 
 * whether the datasets in the catalog are "alive" or not.
 * This means checking to see if a dataset can return a first record. 
 */
class CatalogDatasetLiveness extends Operation with LazyLogging {
 
  /*
   * Determine whether the datasets listed in 
   * this catalog are "alive" or not.
   */
  override def applyToFunction(function: Function) = {
    ???
  }
  
  /*
   * Given a dataset name, return true if that dataset serves a first record.
   * Else, false. 
   */
  def urlIsAlive(name: String): Boolean = {
    val baseUrl = "http://lasp.colorado.edu/lisird3/latis/" //TODO: generalize this 
    
    val url = new URL(baseUrl + name)
    val con = url.openConnection.asInstanceOf[HttpURLConnection]
    logger.info("Testing url: " + url.toString)
    
    var is: InputStream = null
    var br: BufferedReader = null
    
    try {
      is = con.getInputStream
      br = new BufferedReader(new InputStreamReader(is))
      
      br.readLine match {
        case "" => false
        case "LaTiS Error: {" => false
        case _ => true
      }
    }
    catch {
      case e: java.lang.Exception => false
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