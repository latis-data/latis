package latis.server

import javax.servlet.http.{ HttpServlet, HttpServletRequest, HttpServletResponse }
import latis.util.LatisProperties
import latis.reader.tsml.TsmlReader
import latis.metadata.Registry
import latis.util.LatisServerProperties
import latis.writer.HttpServletWriter
import com.typesafe.scalalogging.slf4j.Logging

class LatisServer extends HttpServlet with Logging {

  override def init() {
    logger.info("Initializing LatisServer.")
    LatisProperties.init(getServletConfig)
  }

  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    var reader: TsmlReader = null //hack so it's visible to finally

    try {
      logger.debug("Processing request: " + request.getRequestURL())

      //Get the request not including the constraints
      val path = request.getPathInfo()

      //Get the type of the output request from the dataset suffix.
      val index = path.lastIndexOf(".");
      val suffix = path.substring(index + 1); //suffix
      val dsname = path.substring(1, index); //dataset name, drop leading "/"

      logger.debug("Locating dataset: " + dsname)
      
      //Get Dataset
      val url = Registry.getTsmlUrl(dsname)
      
      logger.debug("Reading dataset from TSML: " + url)
      reader = TsmlReader(url)

      val constraints = DapConstraintParser.parseQuery(request.getQueryString)
      val dataset = reader.getDataset(constraints)

      logger.debug("Writing " + suffix + " dataset.")
      val writer = HttpServletWriter(response, suffix)

      writer.write(dataset)
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