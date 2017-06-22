package latis.reader.tsml.ml

import java.io.File
import java.io.FileNotFoundException
import java.net.URL
import scala.xml.Elem
import scala.xml.Node
import scala.xml.ProcInstr
import scala.xml.XML
import latis.util.LatisProperties
import scala.xml.transform.RewriteRule
import scala.xml.UnprefixedAttribute
import scala.xml.transform.RuleTransformer
import scala.xml.Null
import scala.xml.MetaData
import scala.xml.NodeSeq.Empty
import java.net.URI


/**
 * Convenient wrapper for dealing with the TSML XML.
 */
class Tsml(val xml: Elem) { 
  
  /**
   * Pull the &lt;dataset&gt; element from the XML, and wrap it in a DatasetMl
   * class. If the dataset element does not define an adapter, use the TsmlResolver
   * to look for another tsml file with the dataset's name.
   */
  lazy val dataset: DatasetMl = xml \ "adapter" match {
    case Empty => xml \ "@ref" match {
      case Empty => throw new Exception("Tsml does not define an adpater or a name for this dataset.")
      case e => {
        if(new URI(e.text).isAbsolute) TsmlResolver.fromUrl(new URL(e.text)).dataset
        else TsmlResolver.fromName(e.text).dataset
      }
    }
    case e => new DatasetMl(xml) //assumes only one "dataset" element
  }
  
  /**
   * Get all the processing instructions (ProcInstr) for the Dataset
   * as a Seq of the type (target) and String value (proctext).
   */
  lazy val processingInstructions: Seq[(String,String)] = { 
    val pis: Seq[ProcInstr] = dataset.xml.descendant.flatMap(_ match {
      case pi: ProcInstr => Some(pi)
      case _ => None
    })
    pis.map(pi => pi.target -> pi.proctext)
  }
  
  def getVariableAttribute(vname: String, attribute: String): String = dataset.findVariableMl(vname) match {
    case None => throw new Exception(s"Could not find VariableMl with name '$vname'")
    case Some(ml) => {
      val atts = ml.getAttributes ++ ml.getMetadataAttributes
      atts(attribute)
    }
  }
  
  /**
   * Look for an attribute for the variable with the given name.
   * Try XML attributes of the variable's element first, then try 
   * its metadata element. Return an Option so we can better handle
   * cases where no such variable or attribute is defined.
   */
  def findVariableAttribute(vname: String, attribute: String): Option[String] = dataset.findVariableMl(vname) match {
    case None => None
    case Some(ml) => {
      //try xml attributes for the variable element
      ml.getAttributes.get(attribute) match {
        case s: Some[String] => s  //found it in variable attributes
        case None => {
          //else try metadata attributes
          ml.getMetadataAttributes.get(attribute) match {
            case s: Some[String] => s  //found it in metadata attributes
            case None => None  //attribute not found for the given variable
          }
        }
      }
    }
  }

  override def toString: String = xml.toString
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
      case null => Tsml(xml) //no ref, use top level dataset element
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
