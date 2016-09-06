package latis.server

import java.net.URLDecoder
import com.typesafe.scalalogging.LazyLogging
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import latis.dm.Dataset
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.TsmlResolver
import latis.util.LatisProperties
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter
import latis.writer.OverviewWriter
import latis.reader.DatasetAccessor

class LatisServer extends HttpServlet with LazyLogging {

  override def init() {
    logger.info("Initializing LatisServer.")
    LatisProperties.init(new LatisServerProperties(getServletConfig))
    //TODO: should we reload properties with every request?
  }
  
  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    //Need to expose outside of try scope:
    var reader: DatasetAccessor = null
    var dataset: Dataset = null
    
    try {
      //Get the request not including the constraints.
      val path = request.getPathInfo
      val query = request.getQueryString match {
        case s: String => {
          val parts = s.split("&")
          parts.map(x => URLDecoder.decode(x, "UTF-8"))
          }
        case _ => Array("")
      }
      val operations = (new DapConstraintParser).parseArgs(query)
      
      // If someone requests "/latis" redirect them to
      // "/latis/" (the Catalog page)
      if (path == null || path.equals("")) {
        response.sendRedirect(request.getRequestURI() + "/")
        return
      }
      
      // Quick short-circuit for the case when someone requests
      // http://[base]/latis/
      // In this case we want to return a short HTML overview
      // of the current LaTiS install
      val isPathEmpty = path.equals("/")
      val isQueryEmpty = query.equals("")
      if (isPathEmpty && isQueryEmpty) {
        logger.info("Processing OverviewWriter request (no path or query)")
        OverviewWriter(getServletConfig).write(request, response)
        return;
      }
      
      logger.info("Processing request: " + path + "?" + query)

      //Get the dataset name and type of the output request from the dataset suffix.
      val index = path.lastIndexOf(".");
      val suffix = if(index < 0) "html" else path.substring(index + 1); //suffix for Writer
      val dsname = if(index < 0) path.substring(1) else path.substring(1, index); //dataset name, drop leading "/"

      //Get the DatasetAccessor for the requested dataset.
      logger.debug("Locating dataset accessor: " + dsname)
      
      reader = DatasetAccessor.fromName(dsname)
      
      //Get the Dataset from the DatasetAccessor. 
      // operations was produced by parseArgs at the beginning
      dataset = reader.getDataset(operations)
      
      //Make the Writer, wrapped for Servlet output.
      logger.debug("Writing " + suffix + " dataset.")
      val writer = HttpServletWriter(response, suffix)
      //TODO: if suffix not supported, return "415 Unsupported Media Type"

      //Write the dataset. 
      //Note, data might not be read until the Writer asks for it.
      //  So don't blame the Writer if this seems slow.
      //Handle IOException when user aborts request.
      //TODO: are we at risk of catching/masking other sources of IOException?
      //TODO: but reading is usually lazy so this would catch dataset reading errors too, which do need an error response
      //try {
        writer.write(dataset)
      //} catch {
      //  case e: IOException => logger.warn("IOException during write: " + e.getMessage)
      //}
      
      logger.info("Request complete.") //TODO: "with status...", do in finally?

    } catch {
      //Handle client abort (broken pipe).
      //This is not critical but we shouldn't log an error in this case.
      //Glassfish and Tomcat throw org.apache.catalina.connector.ClientAbortException
      //  which extends IOException with the message: "Broken pipe"
      //TODO: need to be app server agnostic, don't want to add catalina.jar dependency
      //Do in try/catch block around write above
//      case cae: org.apache.catalina.connector.ClientAbortException => {
//        logger.warn("ClientAbortException: " + cae.getMessage)
//      }
      case e: Throwable => {
        //TODO: throw internal and external exceptions with messages to print here
        //TODO: safe to catch Thowable? e.g. get out of memory errors, we are on our way out anyway
        //  if OOM, try to free some resources so we can at least serve an error message?
        
        logger.error("Exception in LatisServer: " + e.getMessage, e)
        
        //Return an error response.
        //TODO: Use the Writer mapped with the "error" suffix in the latis properties?       
        //TODO: deal with exceptions thrown after writing starts
        val writer = ErrorWriter(response)
        writer.write(e)
        
      }
      
    } finally {
      //Let Reader know that it can release resource (e.g. open files, database connections).
      try{ reader.close } catch { case e: Exception => }
    }

  }

}