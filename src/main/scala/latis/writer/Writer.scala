package latis.writer

import latis.dm.Dataset
import latis.util.LatisProperties

import java.io.File
import java.io.OutputStream

import scala.collection.immutable
import scala.collection.mutable

/**
 * Base class for Dataset writers.
 */
abstract class Writer {
  
  //---- Define abstract write method -----------------------------------------
  
  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset): Unit 
  
  
  //---- Writer properties from latis.properties ------------------------------
  
  /**
   * Store XML attributes for this Adapter definition as a properties Map.
   */
  private var properties: immutable.Map[String,String] = immutable.Map[String,String]()

  /**
   * Return Some property value or None if property does not exist.
   */
  def getProperty(name: String): Option[String] = properties.get(name)
  
  /**
   * Return property value or default if property does not exist.
   */
  def getProperty(name: String, default: String): String = getProperty(name) match {
    case Some(v) => v
    case None => default
  }
  
  
  //---- Manage the File or OutputStream to write to --------------------------

  //Allow writer to be assigned an output stream or file to write to.
  //Only allow other Writers in this package to change output. May need to revisit.
  private var outputStream: OutputStream = null
  private[writer] def setOutputStream(out: OutputStream) = {
    //Make sure we have only one output option. Last one wins.
    file = null
    this.outputStream = out //TODO: set file to null?
  }
  def getOutputStream = outputStream
  
  private[writer] var file: File = null
  private[writer] def setFile(file: File) = {
    //Make sure we have only one output option. Last one wins.
    outputStream = null
    this.file = file
  }
  
  def getFile: File = file match {
    case null => null //TODO: if file is null, dataset.getName + suffix, but don't have access to dataset
    case f: File => f
  }
  
//  def getFileName = getFile match {
//    case f: File => file.getName
//    case _ => null
//  }

  
  //---- Define mime type for server output -----------------------------------
    
  /**
   * Return the mime type of the output format.
   * Needed for the Servlet Writer.
   * Default to application/octet-stream per the http spec:
   * http://www.w3.org/Protocols/rfc2616/rfc2616-sec7.html#sec7.2.1
   */
  def mimeType: String = "application/octet-stream" 
  //TODO: have Writers extend traits (e.g. Text, Binary) and inherit mime type?
  //TODO: get from latis.properties?
}


//==== Companion Object =======================================================

object Writer {
  
  /**
   * Construct a Writer of the type specified by the suffix
   * and an OutputStream to write to.
   */
  def apply(out: OutputStream, suffix: String): Writer = {
    val writer = fromSuffix(suffix)
    writer.outputStream = out
    writer
  }
  
  /**
   * Construct a Writer with the name of a file to write to.
   * Use the file suffix to determine the type of Writer.
   */
  def apply(fileName: String) : Writer = {
    val suffix = fileName.split(".").last
    val writer = Writer.fromSuffix(suffix)
    writer.file = new File(fileName)
    writer
  }
  
  /**
   * Construct Writer of the type given by the suffix
   * as defined in latis.properties.
   */
  def fromSuffix(suffix: String): Writer = {
    LatisProperties.get("writer." + suffix + ".class") match {
      case Some(cname) => {
        val writer = fromClass(cname)
        //add properties from the writer definition in latis.properties
        //writer.properties 
        val props = mutable.Map[String, String]()
        val keys = LatisProperties.keys.filter(_.startsWith("writer." + suffix))
        for (key <- keys) {
          val k = key.substring(suffix.length + 8) //skip "writer.sfx."
          props += (k -> LatisProperties(key))
        }
        
        //add the suffix to the properties, too
        props += ("suffix" -> suffix)
        
        writer.properties = props.toMap //make it immutable
        writer.outputStream = System.out  //default to standard out
        writer
      }
      case None => throw new RuntimeException("Unsupported Writer suffix: " + suffix)
    }
  }
  
  /**
   * Construct writer from class name using reflection.
   */
  def fromClass(cname: String): Writer = {
    val cls = Class.forName(cname)
    val ctor = cls.getConstructor()
    ctor.newInstance().asInstanceOf[Writer]
  }
  
}
