package latis.reader.tsml.ml

import scala.xml.Node

/**
 * Wrapper for TSML that defines a Tuple Variable.
 */
class TupleMl(xml: Node) extends VariableMl(xml) {
  
  lazy val variables = Tsml.getVariableNodes(xml).map(VariableMl(_))
  
}
