package latis.server

import java.net.URLDecoder
import com.typesafe.scalalogging.LazyLogging
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import latis.dm.Dataset
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.TsmlResolver
import latis.util.LatisServerException
import latis.util.LatisProperties
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter
import latis.writer.OverviewWriter
import latis.reader.DatasetAccessor
import latis.util.CacheManager

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
      //Split the query string by "&" and make an Operation for each constraint.
      val queryArgs = request.getQueryString match {
        case s: String => s.split("&").map(x => URLDecoder.decode(x, "UTF-8"))
        case _ => Array[String]() //empty array
      }
      val operations = (new DapConstraintParser).parseArgs(queryArgs)
      
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
      val isQueryEmpty = queryArgs.isEmpty
      if (isPathEmpty && isQueryEmpty) {
        logger.info("Processing OverviewWriter request (no path or query)")
        OverviewWriter(getServletConfig).write(request, response)
        return;
      }
      
      logger.info("Processing request: " + path + "?" + queryArgs.mkString("&"))

      //Get the dataset name and type of the output request from the dataset suffix.
      val index = path.lastIndexOf(".");
      val suffix = if(index < 0) "html" else path.substring(index + 1); //suffix for Writer
      val dsname = if(index < 0) path.substring(1) else path.substring(1, index); //dataset name, drop leading "/"

      //Remove dataset from cache if older than header's "Cache-Control: max-age"
      //TODO: make sure another request isn't using it?
      request.getHeader("Cache-Control") match {
        case null => //no-op
        case cc => cc.split("=") match {
          case Array("max-age", s) =>
            val maxage = s.toDouble //seconds
            CacheManager.getDataset(dsname)
                        .map(_.getMetadata("creation_time")) match {
                //TODO: what if "creation_time" is not defined, whack?
                //      have the CacheManager add the metadata?
                case Some(s) =>
                  val age = (System.currentTimeMillis - s.toLong).toDouble / 1000 //seconds
                  if (age > maxage) {
                    logger.info("Clearing cache for dataset: " + dsname)
                    CacheManager.removeDataset(dsname)
                  }
                case None => //dataset not cached
              }
          case _ => //not a header we deal with
        }
      }
      
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
      case lse: LatisServerException => {
        logger.warn("LatisServerException in LatisServer: " + lse.getMessage, lse)
        handleError(response, lse)
      }
      case uoe: UnsupportedOperationException => {
        logger.warn("UnsupportedOperationException in LatisServer: " + uoe.getMessage, uoe)
        handleError(response, uoe)
      }
      case e: Throwable => {
        //TODO: throw internal and external exceptions with messages to print here
        //TODO: safe to catch Thowable? e.g. get out of memory errors, we are on our way out anyway
        //  if OOM, try to free some resources so we can at least serve an error message?
        
        logger.error("Exception in LatisServer: " + e.getMessage, e)
        handleError(response, e)
      }
      
    } finally {
      //Let Reader know that it can release resource (e.g. open files, database connections).
      try{ reader.close } catch { case e: Exception => }
    }

  }
  
  def handleError(r: HttpServletResponse, e: Throwable) {
    // If the response is "committed" (i.e. if the headers have already
    // been sent to the client and we've started writing the response),
    // then we can't use the ErrorWriter because it needs to set the
    // HTTP Status Code and a few extra headers, and you can't do that
    // once you've started writing the response.
    if (!r.isCommitted()) {
      //Return an error response.
      //TODO: Use the Writer mapped with the "error" suffix in the latis properties?       
      //TODO: deal with exceptions thrown after writing starts
      val writer = ErrorWriter(r)
      writer.write(e)
    }
  }

}