package latis.reader.tsml

import scala.xml._

class TupleMl(xml: Node) extends VariableMl(xml) {
  
  lazy val variables = getVariableNodes(xml).map(VariableMl(_))
  
  def getVariableMl() = variables
//  def getVariableMl(): Seq[VariableMl] = {
//    val es = xml.child.filter(_.isInstanceOf[Elem])
//    es.map(VariableMl(_))
//  }
}