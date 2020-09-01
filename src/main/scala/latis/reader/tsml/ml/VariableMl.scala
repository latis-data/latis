package latis.reader.tsml.ml

import latis.util.StringUtils

import scala.collection.Map
import scala.collection.Seq
import scala.collection.mutable
import scala.xml.Attribute
import scala.xml.Elem
import scala.xml.Node
import scala.xml.NodeSeq.seqToNodeSeq

/**
 * Wrapper for an Element within the TSML that represents a Variable.
 */
abstract class VariableMl(xml: Node) {
  
  def label: String = xml.label
  
  /**
   * Get the value of this element's attribute with the given name.
   */
  def getAttribute(name: String): Option[String] = {
    (xml \ ("@"+name)).text match {
      case s: String if s.length > 0 => Some(s)
      case _ => None
    }
  }
  
  def getAttributes: Map[String, String] = {
    val atts = xml.attributes  //(xml \ ("@*"))
    atts.map(att => (att.asInstanceOf[Attribute].key -> att.asInstanceOf[Attribute].value.text)).toMap
  }
  
  /**
   * Get the text content of this element.
   */
  def getContent(): Option[String] = {
    xml.child.find(_.isInstanceOf[scala.xml.Text]) match {
      case Some(text) => Some(text.text.trim())
      case None => None
    }
  }
  
  /**
   * Find the first Element with the given label.
   */
  def getElement(label: String): Option[Elem] = {
    val nodes = xml \ label
    nodes.length match {
      case 0 => None
      case _ => Some(nodes.head.asInstanceOf[Elem])
    }
  }
  
  /**
   * Get all the metadata attributes from the tsml for this Variable as key/value pairs.
   */
  def getMetadataAttributes: Map[String, String] = {
    //Gather the XML attributes from the "metadata" element for this Variable.
    val map = mutable.HashMap[String,String]()
    val seq = for {
      e <- xml \ "metadata"
      att <- e.attributes
    } yield (att.key, StringUtils.resolveParameterizedString(att.value.text))
    Map[String, String](seq: _*)
  }
  
  def hasName(name: String): Boolean = {
    val names = ((xml \ "@id").map(_.text)) ++ //id attribute
      (((xml \ "metadata").flatMap(_ \ "@name")).map(_.text)) :+ //metadata element name
      label //implicit names
    names.contains(name)
  }
  
  def getName: String = {
    val names = ((xml \ "@id").map(_.text)) ++ //id attribute
      (((xml \ "metadata").flatMap(_ \ "@name")).map(_.text)) :+ //metadata element name
      label //implicit names
    names.head
  }
  
  /**
   * Find a VariableMl with the given name.
   */
  def findVariableMl(name: String): Option[VariableMl] = {
    if (hasName(name)) Some(this)
    else if(this.isInstanceOf[ScalarMl]) None
    else (xml \ "_").flatMap(VariableMl(_).findVariableMl(name)).headOption
  }

  override def toString: String = xml.toString
}

object VariableMl {
   
  def apply(xml: Node): VariableMl = {
    xml.label match {
      case "tuple" => new TupleMl(xml)
      case "function" => new FunctionMl(xml)
      case "time" => new TimeMl(xml)
      case _ => new ScalarMl(xml)
    }
  }
  
  /**
   * If more than one, wrap in TupleMl.
   */
  def apply(es: Seq[Node]): VariableMl = es.length match {
    case 1 => VariableMl(es.head) //only one
    case _ => new TupleMl(<tuple/>.copy(child = es)) //implicit Tuple
  }
  
}
