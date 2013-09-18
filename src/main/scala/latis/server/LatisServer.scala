package latis.server

import javax.servlet.http.{ HttpServlet, HttpServletRequest, HttpServletResponse }
import latis.util.LatisProperties
import latis.reader.tsml.TsmlReader
import latis.metadata.Registry
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter
import com.typesafe.scalalogging.slf4j.Logging
import java.net.URLDecoder

class LatisServer extends HttpServlet with Logging {

  override def init() {
    logger.info("Initializing LatisServer.")
    LatisProperties.init(getServletConfig)
  }

  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    var reader: TsmlReader = null //hack so it's visible to finally

    try {
      logger.debug("Processing request: " + request.getRequestURL())

      //Get the request not including the constraints.
      val path = request.getPathInfo()

      //Get the dataset name and type of the output request from the dataset suffix.
      val index = path.lastIndexOf(".");
      val suffix = path.substring(index + 1); //suffix for Writer
      val dsname = path.substring(1, index); //dataset name, drop leading "/"

      //Get the URL to the Dataset's TSML descriptor.
      logger.debug("Locating dataset: " + dsname)
      val url = Registry.getTsmlUrl(dsname)
      
      logger.debug("Reading dataset from TSML: " + url)
      //Construct the reader for this Dataset.
      reader = TsmlReader(url)

      //Get the query string from the request.
      val query = request.getQueryString match {
        case s: String => URLDecoder.decode(s, "UTF-8")
        case _ => ""
      }
      
      //Manage the query string "arguments"
      //Separate out writer instructions of the form name=value.
      //TODO: just use those left over after reader?
      val args = query.split("&")
      val (writerArgs, datasetArgs) = args.partition(_.matches("""\w+=\w+"""))
      
      //Get the Dataset from the Reader. 
      //Note, pass all args to reader so we can continue to support "=" for selections.
      //TODO: deprecate "=" for selections, use "==", but violates DAP2 spec? and users expect it (e.g. sql)
      val constraints = DapConstraintParser.parseArgs(args)
      val dataset = reader.getDataset(constraints)

      //Make the Writer, wrapped for Servlet output.
      logger.debug("Writing " + suffix + " dataset.")
      val writer = HttpServletWriter(response, suffix)

      //Write the dataset. 
      //Note, data might not be read until the Writer asks for it.
      //  So don't blame the Writer if this seems slow.
      writer.write(dataset, writerArgs)
      logger.debug("Request complete.")

    } catch {
      
      case e: Exception => {
        logger.error("Exception in LatisServer", e)
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