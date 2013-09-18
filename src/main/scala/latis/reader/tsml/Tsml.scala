package latis.reader.tsml

import java.net.URL
import scala.xml._
import java.io.File
import scala.collection.mutable.ArrayBuffer


/**
 * Convenient wrapper for dealing with the TSML XML.
 */
class Tsml(tsml: Elem) { //extends VariableMl(tsml) { //TODO: should this extend VariableMl? not a Variable
  
  lazy val dataset = new DatasetMl((tsml \ "dataset")(0)) //assumes only one "dataset" element
  
  /**
   * Gather the Names of all named Variables.
   */
  def getVariableNames: Seq[String] = {
    //TODO: assumes only scalars are named, for now
    Tsml.getVariableNodes((tsml \ "dataset").head).flatMap(Tsml.getVariableName(_))
  }

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
    val pis: Seq[ProcInstr] = (tsml \ "dataset")(0).child.filter(_.isInstanceOf[ProcInstr]).map(_.asInstanceOf[ProcInstr])
    val pimap: Map[String, Seq[ProcInstr]] = pis.groupBy(_.target) //put into Map by target name
    pimap.map((pair) => (pair._1, pair._2.map(_.proctext))) //change Seq of PIs to Seq of their text values
    //TODO: do we need to override this Map's "default" to return an empty Seq[String]?
  }

}

object Tsml {
  
  def apply(url: URL): Tsml = {
    val xml = XML.load(url)
    //If the URL includes a reference ("#" anchor), include only the referenced dataset.
    url.getRef() match {
      case null => new Tsml(xml) //no ref, use top level dataset element
      case ref: String => {
        (xml \\ "dataset").find(node => (node \ "@name").text == ref) match {
          case Some(node) => new Tsml(<tsml>node.head</tsml>) //TODO: will this form work?
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
    else "file:" + scala.util.Properties.userDir + File.separator + path //relative file path
    Tsml(new URL(url))
  }
  
    
  def getVariableNodes(vnode: Node): Seq[Node] = vnode.child.filter(isVariableNode(_))
  
  def isVariableNode(node: Node): Boolean = {
    val exclusions = List("metadata", "adapter", "values") //valid xml Elements that are not Variables
    node.isInstanceOf[Elem] && (! exclusions.contains(node.label)) 
  }
  
  def getVariableName(node: Node): Option[String] = {
    (node \ "metadata@name").text match {
      case "" => {
        //not defined in metadata element, try attribute
        (node \ "@name").text match {
          case "" => None
          case name: String => Some(name)
        }
      }
      case name: String => Some(name)
    }
  }
}
