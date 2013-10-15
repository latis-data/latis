package latis.reader.tsml.ml

import scala.xml._
import latis.util.Util

class DatasetMl(xml: Node) extends TupleMl(xml) {
  
  //deal with adapter and implicit time series
  override def getVariableMl: Seq[VariableMl] = {
    //get all direct child elements that represent variables
    val es = Tsml.getVariableNodes(xml)
    
    //Handle implicit Functions (index or time assumed to be a 1D domain)
    //Note: order doesn't matter, they don't have to be first.
    if (Tsml.hasChildWithLabel(xml, "index")) es.partition(_.label == "index") match {
      //TODO: make IndexFunction, iterator counter for index value
      //TODO: error if more than one Index?
      //Make implicit Function with Index domain.
      case (index, r) => Seq(wrapWithImplicitFunction(index, r))
      //TODO: error case _ =>
    } else if (Tsml.hasChildWithLabel(xml, "time")) es.partition(_.label == "time") match {
      //deal with implicit time series: "time" label.
      case (time, r) => Seq(wrapWithImplicitFunction(time, r))
      //TODO: error case _ =>
    } else {
      es.map(VariableMl(_))
    }
  }
  
  //TODO: TsmlUtils?
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
    val seq = for (att <- atts) yield (att.key, Util.resolveParameterizedString(att.value.text))
    seq.toMap
  }
}
