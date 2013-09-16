package latis.writer

import latis.dm.Dataset
import javax.servlet.http.HttpServletResponse

/**
 * Decorate a Writer to write via a ServletResponse.
 */
class HttpServletWriter(writer: Writer, response: HttpServletResponse) extends Writer {
  //TODO: consider writers that can't stream, write tmp file
  //  or simply serve an existing file!?

  
  def write(dataset: Dataset, args: Seq[String]): Unit = {
  //def write(dataset: Dataset) {
    //write http header stuff
            
    //Set the Content-Type HTTP header
    response.setContentType(writer.getMimeType());
        
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
    val writer = Writer(response.getOutputStream(), suffix)
    new HttpServletWriter(writer, response)
  }
}