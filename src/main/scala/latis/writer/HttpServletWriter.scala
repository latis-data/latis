package latis.writer

import latis.dm.Dataset
import javax.servlet.http.HttpServletResponse
import latis.util.LatisProperties

/**
 * Decorate a Writer to write via a ServletResponse.
 */
class HttpServletWriter(writer: Writer, response: HttpServletResponse) extends Writer {
  
  def write(dataset: Dataset): Unit = {
    //TODO: write other http headers
            
    //Define the allowed origin for cross-origin resource sharing (CORS)
    LatisProperties.get("cors.allow.origin") match {
      case Some(s) => response.addHeader("Access-Control-Allow-Origin", s)
      case None => 
    }

    //Provide a file name based on the Dataset name.
    //May be different from the dataset identifier used in the request URL.
    //TODO: generalize for all formats, need to get suffix from writer
    writer match {
      case _: CsvWriter => {
        val fileName = dataset.getName + ".csv"
        response.addHeader("Content-Disposition", "inline; filename=\"" + fileName)
      }
      case _ =>
    }
    
    writer.write(dataset)
    
    response.setStatus(HttpServletResponse.SC_OK);
    response.flushBuffer()
  }
}

object HttpServletWriter {
  
  def apply(response: HttpServletResponse, suffix: String) = {
    //Set the Content-Type HTTP header before we get the writer from the response.
    //TODO: but it seems to have been working, minus the character encoding
    //  could we go back to the cleaner design of passing output stream to writer constructor?
    val writer = Writer.fromSuffix(suffix)
    response.setContentType(writer.mimeType)
    //TODO: why do we still need to set character encoding? 
    //response.setCharacterEncoding("UTF-8") //is this required? maybe ISO-8859-1
    response.setCharacterEncoding("ISO-8859-1")
    writer.setOutputStream(response.getOutputStream)
    new HttpServletWriter(writer, response)
  }
}