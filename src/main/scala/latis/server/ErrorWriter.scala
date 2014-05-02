package latis.server

import javax.servlet.http.HttpServletResponse
import java.io.PrintWriter

class ErrorWriter(response: HttpServletResponse) {

  def write(e: Throwable) {  
    response.reset //TODO: what are the side effects?
  
    //Note, must set status before getting output stream?
    //TODO: consider more specific errors, this is always 500
    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
    response.setContentType("text/plain")
  
    val writer = new PrintWriter(response.getOutputStream)
    writer.println("LaTiS Error: {")
    writer.println("  " + e.getMessage)
    writer.println("}")
    writer.flush()
    response.flushBuffer()
  }
}

object ErrorWriter {
  def apply(response: HttpServletResponse) = new ErrorWriter(response)
}