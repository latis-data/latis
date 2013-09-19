package latis.writer

import latis.dm.Dataset
import javax.servlet.http.HttpServletResponse
import java.io.OutputStream
import java.io.PrintWriter

class JsonpWriter(out: OutputStream) extends Writer {
  //TODO: consider writers that can't stream, write tmp file
  //  or simply serve an existing file!?
  
  //Get the writer to decorate
  //TODO: use property
  val writer = new CompactJsonWriter(out)
  
  private val _writer = new PrintWriter(out)
  
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

