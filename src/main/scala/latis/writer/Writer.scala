package latis.writer

import latis.dm.Dataset
import java.io.OutputStream
import latis.util.LatisProperties

/**
 * Base class for Dataset writers.
 */
abstract class Writer {

  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset)
  //TODO: return the Writer so we could append a "close"?
  
  /**
   * Release resources acquired by this Writer.
   */
  def close()
  
  /**
   * Return the mime type of the output format.
   * Needed for the Servlet Writer.
   * Default to plain text.
   */
  def getMimeType(): String = "text/plain" 
  //TODO: have Writers extend traits (e.g. Text, Binary) and inherit mime type
  
}

object Writer {
  
  def apply(out: OutputStream, suffix: String): Writer = {
    LatisProperties.get("writer." + suffix + ".class") match {
      case Some(cname) => {
        val cls = Class.forName(cname)
        val ctor = cls.getConstructor(classOf[OutputStream])
        ctor.newInstance(out).asInstanceOf[Writer]
      }
      case None => throw new RuntimeException("Unsupported Writer suffix: " + suffix)
    }
  }
  
  //use System.out as default
  def apply(suffix: String) : Writer = Writer(System.out, suffix)
  
}