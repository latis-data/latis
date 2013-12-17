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
  def write(dataset: Dataset): Unit //= write(dataset, Seq.empty)
  //TODO: return the Writer so we could append a "close"?
  
  //def write(dataset: Dataset, args: Seq[String]): Unit
  
  /**
   * Release resources acquired by this Writer.
   */
  //def close(): Unit  
  /*
   * TODO: 2013-12-11
   * can we just do this? = _out.close
   * problematic since _out was passed to us
   * default is System.out which doesn't need to be closed
   * Servlet output stream should not be closed by us
   * 
   * Do we even need a close method?
   * 
   * Consider writing to file.
   * should caller be responsible for opening output stream?
   * not any would do, if servlet output, need to write tmp file first (e.g. netcdf, idl save)?
   * provide location (URL) akin to readers?
   * 
   * Consider writing to database.
   * doesn't make sense to give dbwriter an output stream?
   * jdbc URL?
   * maybe akin to bcp? but rather restrictive
   * but it does make sense to tell it to close when we are done writing?
   * though write(dataset) can clean up after itself, call only once
   * 
   * mapping suffix to writer is only needed by server
   * that should always apply to what the server returns
   *   not database write, but could return status?
   *   but DSL could
   * subclass of writer that take an output stream: OutputWriter?
   * **  construct with "=> OutputStream" so it can be opened lazily
   * use traits?
   * 
   * Consider writing to tmp file then serving it
   * can you simply stream a file (e.g. netcdf) to a servlet output stream?
   * 
   * any way, see no need for close
   */
  
  /**
   * Return the mime type of the output format.
   * Needed for the Servlet Writer.
   * Default to application/octet-stream per the http spec:
   * http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1
   */
  def mimeType: String = "application/octet-stream" 
  //TODO: have Writers extend traits (e.g. Text, Binary) and inherit mime type
  
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