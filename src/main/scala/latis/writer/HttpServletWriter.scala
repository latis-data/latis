package latis.writer

import latis.dm.Dataset
import javax.servlet.http.HttpServletResponse
import latis.util.LatisProperties

/**
 * Decorate a Writer to write via a ServletResponse.
 */
class HttpServletWriter(writer: Writer, response: HttpServletResponse) extends Writer {
  //TODO: consider writers that can't stream, write tmp file
  //  or simply serve an existing file!?

  
  def write(dataset: Dataset, args: Seq[String]): Unit = {
  //def write(dataset: Dataset) {
    //write http header stuff
            
    
    //Define the allowed origin for cross-origin resource sharing (CORS)
    //TODO: get from properties, compare with Origin in request header? or will browser do that?
    //Just allow everything for now.
//    LatisProperties.get("allow.origin") match {
//      case Some(s) => response.addHeader("Access-Control-Allow-Origin", s)
//      case None => 
//    }
    response.addHeader("Access-Control-Allow-Origin", "*")
        
//TODO: add other headers
//        //Set the Content-Description HTTP header
//        String cd = writer.getContentDescription(); 
//        if (cd == null) cd = "tss-" + type;
//        response.setHeader("Content-Description", cd);
//        
//        //Set date headers
//        long date = System.currentTimeMillis();
//        response.addDateHeader("Date", date);
//        response.addDateHeader("Last-Modified", date);
//        //TODO: use data publish date for Last-Modified? 
//        
//        //Set other HTTP headers
//        String dodsServer = TSSProperties.getProperty("server.dods");
//        response.setHeader("XDODS-Server", dodsServer); 
//        String server = TSSProperties.getProperty("server.tss");
//        response.setHeader("Server", server); 
        
    writer.write(dataset, args)
    
    response.setStatus(HttpServletResponse.SC_OK);
    response.flushBuffer()
  }
  
  def close() = writer.close()

}

object HttpServletWriter {
  
  def apply(response: HttpServletResponse, suffix: String) = {
    //Set the Content-Type HTTP header before we get the writer from the response.
    //TODO: but it seems to have been working, minus the character encoding
    //  could we go back to the cleaner design of passing output stream to writer constructor?
    val writer = Writer.fromSuffix(suffix)
    response.setContentType(writer.mimeType)
    //TODO: why do we still need to set character encoding? 
    response.setCharacterEncoding("UTF-8") //is this required? maybe ISO-8859-1 (as seen from TSDS)
    writer._out = response.getOutputStream()
    new HttpServletWriter(writer, response)
  }
}