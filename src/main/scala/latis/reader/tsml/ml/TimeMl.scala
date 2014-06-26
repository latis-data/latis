package latis.reader.tsml.ml

import scala.xml.Node

/**
 * Wrapper for TSML that defines a Time Variable.
 */
class TimeMl(xml: Node) extends VariableMl(xml) {
  
  def getType = getAttribute("type") match {
    case Some(s) => s
    case None    => "real" //default to real times
  }
}
