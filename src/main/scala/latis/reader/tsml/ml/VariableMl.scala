package latis.reader.tsml.ml

import scala.xml._
import scala.collection._

/**
 * Wrapper for an Element within the TSML that represents a Variable.
 */
abstract class VariableMl(xml: Node) {
  
  def label = xml.label
  
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
   * Shortcut to directly get an attribute value
   */
  //def apply(name: String) = (xml \ ("@"+name)).text
  
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
  
  //getElements(label: String): Seq[Elem] ?
  
  def getMetadataAttributes: Map[String, String] = {
    //Gather the XML attributes from the "metadata" element for this Variable.
    val map = mutable.HashMap[String,String]()
    val seq = for (e <- xml \ "metadata"; att <- e.attributes) yield (att.key, att.value.text)
    Map[String, String](seq: _*)
  }

  //flatten VariableMl tree into Seq, depth first
//  def toSeq: Seq[VariableMl] = this match {
//    case s: ScalarMl => Seq(s)
//    case TupleMl(vars) => vars.foldLeft(Seq[VariableMl]())(_ ++ _.toSeq)
//    case FunctionMl(d,r) => d.toSeq ++ r.toSeq
//  }
  
//  def getName = Tsml.getVariableName(xml) match {
//    case Some(name) => name
//    case None => "" //TODO: something better than empty string?
//  }
  
  
  override def toString = xml.toString
}

object VariableMl {
   
  def apply(xml: Node) = {
    xml.label match {
      case "tuple" => new TupleMl(xml)
      case "function" => new FunctionMl(xml)
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
