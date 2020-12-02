package latis.server

import java.io.PrintWriter

import com.typesafe.scalalogging.LazyLogging
import javax.servlet.http._
import latis.reader.JsonReader3
import latis.writer.{HttpServletWriter, ZipWriter3}

class ZipService extends HttpServlet with LazyLogging {
  
  override def doGet(request: HttpServletRequest, response: HttpServletResponse) = {
    //TODO: return error response
    //TODO: write instructions
    val msg = "The ZipService requires a POST."
    val writer = new PrintWriter(response.getOutputStream)
    writer.println(msg)
    writer.flush
  }
  
  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    //val mimeType = request.getContentType
    //assume json, for now
    
    /*
     * TODO: provide name for dataset and thus zip file?
     * 
     * will streaming start before reading all URLs?
     */
    
    logger.info(s"Processing request: zip")
      
    val ds = JsonReader3(request.getInputStream).getDataset()
    
    //Note: needs ZipWriter3 to get content via URL
    HttpServletWriter(response, "zip3").write(ds)
      
    logger.info("Request complete.")
  }
}
