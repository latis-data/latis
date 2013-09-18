package latis.reader.tsml

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
  //def getAttributes
  
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

  
  override def toString = xml.toString
}

object VariableMl {
  //TODO: reject invalid nodes, Option? 
  def apply(xml: Node) = {
    //val xml = node.asInstanceOf[Elem] 
    //println("Making VariableMl for " + xml.label)
    xml.label match {
      //case "dataset" => new DatasetMl(xml) //explicitly constructed in Tsml
      case "tuple" => new TupleMl(xml)
      case "function" => new FunctionMl(xml)
      //case "scalar" => new ScalarMl(xml)
      case _ => new ScalarMl(xml)
      
//      case "time" => {
//        //add name and type
//        //TODO: needed? Time factory will add them
//  /*
//   * TODO: put name and type in <metadata> instead of tsml attributes?
//   * use "id" and "ref" in attributes?
//   */
//        var atts = xml.attributes
//        atts = Attribute("", "name", "time", atts)
//        atts = Attribute("", "type", "Time", atts)
//        val xml2 = xml.copy(attributes = atts)
//        new ScalarMl(xml2)
//      }
      //TODO: "text"?
      //TODO: default to ScalarMl?
    }
  }
  
  def apply(es: Seq[Node]): VariableMl = {
    //println("VariableMl from Seq " + es)
    es.length match {
    case 1 => new ScalarMl(es(0).asInstanceOf[Elem])
    case n if (n > 1) => {
      val xml = <tuple/>.copy(child = es) //TODO: better way to construct this?
      new TupleMl(xml) 
    }
    }
    //case _ => error
  }
  

}
