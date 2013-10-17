package latis.server

import javax.servlet.http.{ HttpServlet, HttpServletRequest, HttpServletResponse }
import latis.util.LatisProperties
import latis.reader.tsml.TsmlReader
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter
import com.typesafe.scalalogging.slf4j.Logging
import java.net.URLDecoder
import latis.metadata.Catalog

class LatisServer extends HttpServlet with Logging {

  override def init() {
    logger.info("Initializing LatisServer.")
    LatisProperties.init(getServletConfig)
  }

  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    var reader: TsmlReader = null //hack so it's visible to finally

    try {      
      //Get the request not including the constraints.
      val path = request.getPathInfo
      
      //Get the query string from the request.
      val query = request.getQueryString match {
        case s: String => URLDecoder.decode(s, "UTF-8")
        case _ => ""
      }
      
      logger.info("Processing request: " + path + "?" + query)

      //Get the dataset name and type of the output request from the dataset suffix.
      val index = path.lastIndexOf(".");
      val suffix = path.substring(index + 1); //suffix for Writer
      val dsname = path.substring(1, index); //dataset name, drop leading "/"

      //Get the URL to the Dataset's TSML descriptor.
      logger.debug("Locating dataset: " + dsname)
      val url = Catalog.getTsmlUrl(dsname)
      
      logger.debug("Reading dataset from TSML: " + url)
      //Construct the reader for this Dataset.
      reader = TsmlReader(url)

      //Convert the query arguments into a mutable collection of Operations.
      //Adapters should remove Operations from this if they handle them
      //  passing the rest for others to handle.
      val args = query.split("&")
      val operations = DapConstraintParser.parseArgs(args)
      
      //Get the Dataset from the Reader. 
      val dataset = reader.getDataset(operations)
      
      //Make the Writer, wrapped for Servlet output.
      logger.debug("Writing " + suffix + " dataset.")
      val writer = HttpServletWriter(response, suffix)

      //Write the dataset. 
      //Note, data might not be read until the Writer asks for it.
      //  So don't blame the Writer if this seems slow.
      //TODO: pass remaining operations to the Writer
      writer.write(dataset)
      
      logger.info("Request complete.") //TODO: "with status...", do in finally?

    } catch {
      
      case e: Exception => {
        //TODO: throw internal and external exceptions with messages to print here, 
        //  as opposed to logging within?
        //  but what about debug logging...?
        
        logger.error("Exception in LatisServer: " + e.getMessage, e)
        //Return error status 500, if all else fails
        response.reset
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "LaTiS was not able to fulfill this request.")
      }
      
    } finally {
      //Let Reader know that it can release resource (e.g. open files, database connections).
      try{ reader.close } catch { case e: Exception => }
    }

  }

}