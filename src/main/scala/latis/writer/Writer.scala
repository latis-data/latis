package latis.writer

import latis.dm.Dataset
import java.io.OutputStream
import latis.util.LatisProperties

/**
 * Base class for Dataset writers.
 */
abstract class Writer {

  var _out: OutputStream = null //TODO: restrict to subclasses, e.g. HttpServletWriter
  def outputStream = _out

  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset): Unit = write(dataset, Seq.empty)
  //TODO: return the Writer so we could append a "close"?
  
  def write(dataset: Dataset, args: Seq[String]): Unit
  
  /**
   * Release resources acquired by this Writer.
   */
  def close(): Unit
  
  /**
   * Return the mime type of the output format.
   * Needed for the Servlet Writer.
   * Default to plain text.
   */
  def mimeType: String = "text/plain" 
  //TODO: have Writers extend traits (e.g. Text, Binary) and inherit mime type
    //TODO: mimeType?
  
}

object Writer {
  
  def apply(out: OutputStream, suffix: String): Writer = {
    val writer = fromSuffix(suffix)
    writer._out = out
    writer
  }
  
  //use System.out as default
  def apply(suffix: String) : Writer = Writer(System.out, suffix)
  
  
  def fromSuffix(suffix: String): Writer = {
    LatisProperties.get("writer." + suffix + ".class") match {
      case Some(cname) => fromClass(cname)
      case None => throw new RuntimeException("Unsupported Writer suffix: " + suffix)
    }
  }
  
  def fromSuffix(suffix: String, out: OutputStream): Writer = {
    val writer = fromSuffix(suffix)
    writer._out = out
    writer
  }
  
  def fromClass(cname: String): Writer = {
    val cls = Class.forName(cname)
    val ctor = cls.getConstructor()
    ctor.newInstance().asInstanceOf[Writer]
  }
  
  def fromClass(cname: String, out: OutputStream): Writer = {
    val writer = fromClass(cname)
    writer._out = out
    writer
  }
}