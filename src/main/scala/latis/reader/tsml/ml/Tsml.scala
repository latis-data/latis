package latis.reader.tsml.ml

import java.io.File
import java.io.FileNotFoundException
import java.net.URL

import scala.xml.Elem
import scala.xml.Node
import scala.xml.ProcInstr
import scala.xml.XML

import latis.util.LatisProperties


/**
 * Convenient wrapper for dealing with the TSML XML.
 */
class Tsml(val xml: Elem) { 
  
  /**
   * Pull the &lt;dataset&gt; element from the XML, and wrap it in a DatasetMl
   * class
   */
  lazy val dataset: DatasetMl = new DatasetMl(xml) //assumes only one "dataset" element
  
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
  lazy val processingInstructions: Map[String, Seq[String]] = { //TODO: currently only searches first level children of "dataset"
    val pis: Seq[ProcInstr] = xml(0).child.filter(_.isInstanceOf[ProcInstr]).map(_.asInstanceOf[ProcInstr])
    val pimap: Map[String, Seq[ProcInstr]] = pis.groupBy(_.target) //put into Map by target name
    pimap.map((pair) => (pair._1, pair._2.map(_.proctext))) //change Seq of PIs to Seq of their text values
    //TODO: do we need to override this Map's "default" to return an empty Seq[String]?
  }

  override def toString = xml.toString
}

object Tsml {
  
  def apply(xml: Node): Tsml = xml match {
    case e: Elem => e.label match {
      case "dataset" => new Tsml(e)
      case _ => throw new RuntimeException("Must construct Tsml from a 'dataset' XML Element.")
    }
    case _ => throw new RuntimeException("Must construct Tsml from a 'dataset' XML Element.")
  }
  
  def apply(url: URL): Tsml = {
    val xml = XML.load(url)
    //If the URL includes a reference ("#" anchor), include only the referenced dataset.
    url.getRef() match {
      case null => new Tsml(xml) //no ref, use top level dataset element
      case ref: String => {
        (xml \\ "dataset").find(node => (node \ "@name").text == ref) match {
          case Some(node) => Tsml(node) 
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
   * If the path is relative and the file isn't found, 
   * this will try prepending the 'dataset.dir' property to the path.
   */
  def apply(path: String): Tsml = {
    //Try using the given path.
    try TsmlResolver.fromPath(path) catch {
      case e: FileNotFoundException => {
        //Try prepending the dataset.dir property
        val dspath = LatisProperties.getOrElse("dataset.dir", "datasets") + File.separator + path
        TsmlResolver.fromPath(dspath)
      }
    }
  }
  
  /**
   * Return the direct children of the input Node that are also
   * Variable Nodes (isVariableNode returns true)
   */
  def getVariableNodes(vnode: Node): Seq[Node] = vnode.child.filter(isVariableNode(_))
  
  /**
   * Return true if the input Node is a Variable node
   * 
   * Currently that means return true if node is a
   * subclass of scala.xml.Elem and is not a &lt;metadata&gt;,
   * &lt;adapter&gt;, or &lt;values&gt; tag. This definition
   * may be updated in the future if we come up with a cleverer
   * check involving xml schema, subtypes, or switch from an
   * exclusions list to an inclusions list.
   */
  def isVariableNode(node: Node): Boolean = {
    //TODO: use xml schema, subtypes?
    //TODO: inclusion list instead of exclusions?
    val exclusions = List("metadata", "adapter", "values") //valid xml Elements that are not Variables
    node.isInstanceOf[Elem] && (! exclusions.contains(node.label)) 
  }
  
}
