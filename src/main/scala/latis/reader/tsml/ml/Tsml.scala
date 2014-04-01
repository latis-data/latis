package latis.reader.tsml.ml

import java.net.URL
import scala.xml._
import java.io.File
import scala.Option.option2Iterable


/**import latis.reader.tsml.ml.DatasetMl

 * Convenient wrapper for dealing with the TSML XML.
 */
class Tsml(val xml: Elem) { //extends VariableMl(xml) { //TODO: should this extend VariableMl? not a Variable
  //Note: XML utilities methods in the companion object are designed for use during VariableMl construction.
  //  TODO: move to TsmlUtils?
  //  Instance methods are designed for use by Adapters after the VariableMl has been constructed.
  //  The goal is for adapters to get what they need from TSML instance methods, 
  //    though they may want to use util methods if they have special xml parsing needs.
  
  lazy val dataset = new DatasetMl((xml \ "dataset").head) //assumes only one "dataset" element
  
//deprecated: adapters should have access to initial Dataset from which they can get variable names
//  /**
//   * Gather the Names of all Scalar Variables.
//   */
//  def getScalarNames: Seq[String] = {
//    val scalars = dataset.toSeq.filter(ml => ml.isInstanceOf[ScalarMl])
//    val names = scalars.map(_.getName) //TODO: error if empty?, .filter(_.nonEmpty)
//    //don't include "index"
//    names.filter(_ != "index")
//    /*
//     * TODO: 2013-10-14
//     * consider source names vs exposed names
//     * These are usually (always?) used to map to source data.
//     * Index is a special case.
//     * Are there other special cases that should be applied here 
//     *   as opposed to operations after the dataset has been constructed?
//     * References?
//     */
//  }
  
  /**
   * Get a sequence of processing instructions' text values (proctext) 
   * for the given type (target).
   */
  def getProcessingInstructions(target: String): Seq[String] = {
    processingInstructions.getOrElse(target, Seq.empty)
  }
  
  /**
   * Get all the processing instructions (ProcInstr) for the Dataset
   * as a Map from the type (target) to a Seq of values (proctext).
   */
  //def getProcessingInstructions(): Map[String, Seq[String]] = {
  lazy val processingInstructions: Map[String, Seq[String]] = {
    val pis: Seq[ProcInstr] = (xml \ "dataset")(0).child.filter(_.isInstanceOf[ProcInstr]).map(_.asInstanceOf[ProcInstr])
    val pimap: Map[String, Seq[ProcInstr]] = pis.groupBy(_.target) //put into Map by target name
    pimap.map((pair) => (pair._1, pair._2.map(_.proctext))) //change Seq of PIs to Seq of their text values
    //TODO: do we need to override this Map's "default" to return an empty Seq[String]?
  }

  override def toString = xml.toString
}

object Tsml {
  
  def apply(xml: Node): Tsml = xml match {
    case e: Elem => e.label match {
      case "tsml" => new Tsml(e)
      case "dataset" => new Tsml(<tsml>{e}</tsml>) //wrap dataset in tsml
      case _ => throw new RuntimeException("Must construct Tsml from a 'tsml' or 'dataset' XML Element.")
    }
    case _ => throw new RuntimeException("Must construct Tsml from a 'tsml' or 'dataset' XML Element.")
  }
  
  def apply(url: URL): Tsml = {
    val xml = XML.load(url)
    //If the URL includes a reference ("#" anchor), include only the referenced dataset.
    url.getRef() match {
      case null => new Tsml(xml) //no ref, use top level dataset element
      case ref: String => {
        (xml \\ "dataset").find(node => (node \ "@name").text == ref) match {
          case Some(node) => new Tsml(<tsml>{node.head}</tsml>) 
          case None => throw new RuntimeException("Can't find dataset with reference: " + ref)
        }
      }
    }
  }
    
  /**
   * Construct a Tsml given the location the TSML.
   * This will accept one of the following forms:
   *   Relative path: A file URL will be used with the present working directory prepended.
   *   Absolute path: (starting with "/") A file URL will be assumed.
   *   Full URL with scheme/protocol (e.g. starts with "http:")
   */
  def apply(path: String): Tsml = {
    val url = if (path.contains(":")) path //already absolute with a scheme
    else if (path.startsWith(File.separator)) "file:" + path //absolute file path
    else getClass.getResource("/"+path) match { //try in the classpath (e.g. "resources")
      case url: URL => url.toString
      case null => "file:" + scala.util.Properties.userDir + File.separator + path //relative file path
    }
    //TODO: resolve relative url from LatisProperties, e.g. dataset.dir
    Tsml(new URL(url))
  }
  
  //just the direct kids
  def getVariableNodes(vnode: Node): Seq[Node] = vnode.child.filter(isVariableNode(_))
    
  def isVariableNode(node: Node): Boolean = {
    //TODO: use xml schema, subtypes?
    //TODO: inclusion list instead of exclusions?
    val exclusions = List("metadata", "adapter", "values") //valid xml Elements that are not Variables
    node.isInstanceOf[Elem] && (! exclusions.contains(node.label)) 
  }
  
  //note, metadata name takes precedence, TODO: dangerous if diff from attribute name
//  def getVariableName(node: Node): Option[String] = (node \ "metadata@name").text match {
//    case "" => {
//      //not defined in metadata element, try attribute
//      (node \ "@name").text match {
//        case "" => node.label match {
//          //try "time" and "index" (with implicit name)
//          case "time" => Some("time")
//          case "index" => Some("index")
//          case _ => None  //no name
//        }
//        case name: String => Some(name) //from attribute
//      }
//    }
//    case name: String => Some(name) //from metadata
//  }
  
//  def hasLabel(xml: Node, label: String): Boolean = xml.find(_.label == label) match {
//    case Some(_) => true
//    case None => false
//  }
  
  //def hasChildWithLabel(xml: Node, label: String): Boolean = xml.child.exists(_.label == label)
  
}
