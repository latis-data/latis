package latis.reader.tsml

import scala.xml._

class FunctionMl(xml: Node) extends VariableMl(xml) {
  
  private var _domain: VariableMl = null
  private var _range: VariableMl = null
  
  val kids = Tsml.getVariableNodes(xml)
  kids.partition(_.label == "domain") match {
    //TODO: match <domain> <range>? or groupBy to get Map by label
    case (d,r) if (d.length == 1) => {
      _domain = VariableMl(Tsml.getVariableNodes(d(0)))
      _range = VariableMl(Tsml.getVariableNodes(r(0))) //TODO: assert that there is one <range>
    }
    case _ => {
      //No "domain" defined, so assume the domain is the first variable
      _domain = VariableMl(kids.head)
      _range = VariableMl(kids.tail)
    }
  }
  
  def domain = _domain
  
  def range = _range

}
