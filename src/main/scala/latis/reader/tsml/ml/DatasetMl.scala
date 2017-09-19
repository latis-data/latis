package latis.reader.tsml.ml

import scala.xml.Node
import latis.util.StringUtils
import scala.xml.UnprefixedAttribute
import scala.xml.Null
import scala.xml.transform.RewriteRule
import scala.xml.Elem
import scala.xml.transform.RuleTransformer
import scala.xml.ProcInstr

/**
 * Wrapper for TSML that defines a Dataset.
 */
class DatasetMl(val xml: Node) extends TupleMl(xml) {
  
//TODO: Dataset is no longer a Tuple
//TODO: deal with aggregation tsml
  
  /**
   * Get a list of top level (i.e. direct children) variable definitions for this dataset node.
   * Handle implicit Functions. If the first variable definition is "index" or "time"
   * wrap in a function with those as a 1D domain.
   * If there are multiple top level variables, wrap in a Tuple.
   */
  def getVariableMl: VariableMl = {
    val es = Tsml.getVariableNodes(xml)
    if (es.isEmpty) throw new Error("TSML does not define any variables.")
    
//TODO: wrap multiple vars in tuple
    //if (es.length > 1) ???

    //Handle implicit Functions. If the first variable definition is "index" or "time"
    // wrap in a function with those as a 1D domain.
    val label = es.head.label //first variable def
    if (label == "index" || label == "time") wrapWithImplicitFunction(es.head, es.tail)
    else VariableMl(es.head)

  }
  
  /**
   * Get all the processing instructions (ProcInstr) for the Dataset
   * as a Seq of the type (target) and String value (proctext).
   */
  lazy val processingInstructions: Seq[(String,String)] = { 
    val pis: Seq[ProcInstr] = xml.child.collect {
      case pi: ProcInstr => pi
    }
    pis.map(pi => pi.target -> pi.proctext)
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
  
  
  /**
   * Create a new Tsml with the adapter.location attribute updated.  
   */
  def setLocation(loc: String): Tsml = {
    val newloc = new UnprefixedAttribute("location", loc, Null)
    val rr = new RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case e: Elem if(e.label == "adapter") => e % newloc
        case other => other
      }
    }
    val rt = new RuleTransformer(rr)
    Tsml(rt.transform(xml).head)
  }
}
