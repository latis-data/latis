package latis.reader.tsml

import scala.xml._
import scala.collection.immutable.HashMap
import scala.util.matching.Regex.Match
import latis.util.LatisProperties
import latis.util.Util

class DatasetMl(xml: Node) extends TupleMl(xml) {
  
  //deal with adapter and implicit time series
  override def getVariableMl: Seq[VariableMl] = {
    //get all direct child elements that represent variables, exclude adapter
    //val es = xml.child.filter(e => e.isInstanceOf[Elem] && e.label != "adapter") 
    val es = getVariableNodes(xml)
    
    //deal with implicit time series: "time" label
    es.partition(_.label == "time") match {
      case (time, r) if (time.length == 1) => { //found one "time" variable
        val domain = <domain/>.copy(child = time(0))  //<domain>time(0)</domain>
        val range = <range/>.copy(child = r)  //<range>r</range>
        val vml = VariableMl(<function/>.copy(child = Seq(domain, range)))
        Seq(vml)
      }
      case _ => es.map(VariableMl(_))
    }
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
