package latis.reader.tsml.ml

import scala.xml.Node
import latis.util.StringUtils

/**
 * Representation of a TSML "dataset" element.
 */
class DatasetMl(xml: Node) extends TupleMl(xml) {
  
  /**
   * Get a list of top level (i.e. direct children) variable definitions for this dataset node.
   * Handle implicit Functions. If the first variable definition is "index" or "time"
   * wrap in a function with those as a 1D domain.
   */
  def getVariableMl: Seq[VariableMl] = {
    val es = Tsml.getVariableNodes(xml)
    
    //Handle implicit Functions. If the first variable definition is "index" or "time"
    // wrap in a function with those as a 1D domain.
    val label = es.head.label
    if (label == "index" || label == "time") Seq(wrapWithImplicitFunction(es.head, es.tail))
    else es.map(VariableMl(_))
  }
  
  /**
   * Given tsml nodes representing the domain and range variables, 
   * wrap them to look like a "function" variable.
   */
  private def wrapWithImplicitFunction(d: Seq[Node], r: Seq[Node]): VariableMl = {
    val domain = <domain/>.copy(child = d.head) //assume one domain variable, <domain>d.head</domain>
    //TODO: make sure r.length > 0
    val range = <range/>.copy(child = r) //the rest, <range>r</range>
    VariableMl(<function/>.copy(child = Seq(domain, range))) //will recurse and make all descendants
  }
  
  /**
   * Put the XML attributes from the "adapter" element into a Map.
   * Any properties parameterized with "${property} will be resolved.
   */
  def getAdapterAttributes(): Map[String,String] = {
    val atts = (xml \ "adapter").head.attributes
    val seq = for (att <- atts) yield (att.key, StringUtils.resolveParameterizedString(att.value.text))
    seq.toMap
  }
}
