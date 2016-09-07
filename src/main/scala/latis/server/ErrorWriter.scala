package latis.server

import javax.servlet.http.HttpServletResponse
import java.io.PrintWriter
import javax.xml.ws.http.HTTPException

class ErrorWriter(response: HttpServletResponse) {

  def write(e: Throwable) = e match {  
    
    //pass along http errors we receive
    case httpe: HTTPException => response.sendError(httpe.getStatusCode, httpe.getMessage)
    
    case _ => {
      if(response.isCommitted()) {
        throw e;
      }
      else {
        response.reset //TODO: what are the side effects?
        //Note, must set status before getting output stream?
        //TODO: consider more specific errors, this is always 500
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        response.setContentType("text/plain")
    
        val errorType = e.getClass.getSimpleName
        val rawMsg = e.getMessage
        val errorMsg = if (rawMsg == null) "" else rawMsg
      
        val writer = new PrintWriter(response.getOutputStream)
        writer.println("LaTiS Error: {")
        writer.println(s"""  $errorType: "$errorMsg"""") // hello world    
        writer.println("}")
        writer.flush()
        response.flushBuffer()
      }
    }
  
  }
}

object ErrorWriter {
  def apply(response: HttpServletResponse) = new ErrorWriter(response)
}