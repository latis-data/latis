package latis.server

import javax.servlet.http.HttpServletResponse
import java.io.PrintWriter

class ErrorWriter(response: HttpServletResponse) {
    /*
     * TODO: treat like any other Writer
     * pluggable via "error" suffix in properties
     * use HttpServletResponse
     * need to pass exception
     * model as Dataset? eew
     * special writer just for server
     * just use same build by suffix
     * but what if user asks for foo.error?
     * pass in dataset, might be handy for context specific errors
     * might want request, too
     */

  response.reset //TODO: what are the side effects?
  response.setContentType("text/plain")
  
  val writer = new PrintWriter(response.getOutputStream)
  
  def write(e: Throwable) {
    writer.println("LaTiS Error: {")
    writer.println("  " + e.getMessage)
    writer.println("}")
    writer.flush()

    //TODO: consider more specific errors
    //response.setStatus(HttpServletResponse.SC_OK)
    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
    response.flushBuffer()
  }
}

object ErrorWriter {
  def apply(response: HttpServletResponse) = new ErrorWriter(response)
}