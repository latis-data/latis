package latis.server

import javax.servlet.http.HttpServletResponse
import java.io.PrintWriter
import latis.util.LatisServiceException
import latis.util.DatasetNotFoundException
//import javax.xml.ws.http.HTTPException

class ErrorWriter(response: HttpServletResponse) {

  def write(e: Throwable): Unit = e match {

    //pass along http errors we receive
    //case httpe: HTTPException => response.sendError(httpe.getStatusCode, httpe.getMessage)
    case dnfe: DatasetNotFoundException => {
      writeWithStatusCode(dnfe, HttpServletResponse.SC_NOT_FOUND) //404
    }
    case iae: IllegalArgumentException => {
      writeWithStatusCode(iae, HttpServletResponse.SC_BAD_REQUEST) //400
    }
    case lse: LatisServiceException => {
      writeWithStatusCode(lse, HttpServletResponse.SC_BAD_REQUEST) //400
    }
    case uoe: UnsupportedOperationException => {
      writeWithStatusCode(uoe, HttpServletResponse.SC_INTERNAL_SERVER_ERROR) //500
    }
    case _ => {
      writeWithStatusCode(e, HttpServletResponse.SC_INTERNAL_SERVER_ERROR) //500
    }

  }
  
  /**
   * Allows writing the error with specific HTTP status codes (400, 500, etc.)
   */
  def writeWithStatusCode(e: Throwable, code: Int): Unit = {
    response.reset //TODO: what are the side effects?
    //Note, must set status before getting output stream?
    response.setStatus(code)
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

object ErrorWriter {
  def apply(response: HttpServletResponse): ErrorWriter = new ErrorWriter(response)
}
