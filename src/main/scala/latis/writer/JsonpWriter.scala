package latis.writer

import latis.dm.Dataset
import javax.servlet.http.HttpServletResponse
import java.io.OutputStream
import java.io.PrintWriter
import latis.util.LatisProperties

class JsonpWriter extends Writer {
  //TODO: consider writers that can't stream, write tmp file
  //  or simply serve an existing file!?
  
  //Get the writer to decorate
  lazy val writer = LatisProperties.get("writer.jsonp.writer") match {
    case Some(cname) => Writer.fromClass(cname, outputStream)
    case None => Writer.fromSuffix("json", outputStream) //default to the json writer
  }
  
  //beware, repurposing the outputStream used by the wrapped Writer
  private lazy val _writer = new PrintWriter(outputStream)
  
  def write(dataset: Dataset, args: Seq[String]) {
    val callback = args.find(_.startsWith("callback")) match {
      case Some(s) => s.split("=")(1)
      case None => throw new RuntimeException("JsonpWriter must have a 'callback' argument.")
    }
    _writer.println(callback + "(")
    _writer.flush()
    
    writer.write(dataset)
    
    _writer.print(");")
    _writer.flush()
  }

  
  override def mimeType: String = "application/json" 
  
  def close = writer.close

}

