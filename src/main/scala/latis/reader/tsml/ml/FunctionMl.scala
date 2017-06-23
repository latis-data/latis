package latis.reader.tsml.ml

import scala.xml.Node

/**
 * Wrapper for TSML that defines a Function Variable.
 * This will fill in the details allowing for briefer syntax in the TSML.
 */
class FunctionMl(xml: Node) extends VariableMl(xml) {
  
  private var _domain: VariableMl = null
  def domain: VariableMl = _domain
  
  private var _range: VariableMl = null
  def range: VariableMl = _range
  
  val kids = Tsml.getVariableNodes(xml)
  kids.partition(_.label == "domain") match {
    //TODO: match <domain> <range>? or groupBy to get Map by label
    case (d,r) if (d.length == 1) => {
      _domain = VariableMl(Tsml.getVariableNodes(d(0)))
      _range = VariableMl(Tsml.getVariableNodes(r(0))) //TODO: assert that there is one <range>
      //TODO: allow range with no domain? implicit index
    }
    case _ => {
      //No "domain" defined, so assume the domain is the first variable
      //TODO: make sure we have at least 2 kids to work with
      _domain = VariableMl(kids.head)
      _range = VariableMl(kids.tail)
    }
  }
}
