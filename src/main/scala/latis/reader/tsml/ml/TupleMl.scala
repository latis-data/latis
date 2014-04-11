package latis.reader.tsml.ml

import scala.xml._

class TupleMl(xml: Node) extends VariableMl(xml) {
  
  lazy val variables = Tsml.getVariableNodes(xml).map(VariableMl(_))
  
  //def getVariableMl() = variables
}

object TupleMl {
  
  //def unapply(tml: TupleMl): Option[Seq[VariableMl]] = Some(tml.variables)
}