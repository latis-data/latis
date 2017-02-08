package latis.ops

import latis.dm.Function
import latis.dm.Tuple
import latis.dm.Variable

/**
 * Splits a Function with a Tuple codomain into a Tuple
 * of scalar Functions. 
 * 
 * Note: Will cause the original Function to be read.
 */
class Split extends Operation {
  
  override def applyToFunction(function: Function): Option[Variable] = function.getRange match {
    case Tuple(vars) => {
      val samples = function.iterator.toSeq //have to read the whole function so that the domain can be repeated
      val dom = samples.map(_.domain)
      val ran = samples.map(_.range.asInstanceOf[Tuple].getVariables).transpose
      Some(Tuple(ran.map(Function(dom, _))))
    }
    case _ => Some(function)
  }
  
}

object Split extends OperationFactory {
  override def apply() = new Split()
}