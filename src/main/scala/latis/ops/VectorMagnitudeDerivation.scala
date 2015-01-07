package latis.ops

import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.implicits.doubleToDataset
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata
import latis.ops.math.MathOperation

class VectorMagnitudeDerivation(names: Seq[String]) extends Operation {
  
  override def applyToFunction(fun: Function): Option[Variable] = {
    Some(Function(fun.getDomain, Tuple(fun.getRange.toSeq :+ Real(Metadata(names.last))), fun.iterator.map(applyToSample(_).get), fun.getMetadata))
  }
  
  override def applyToSample(sample: Sample): Option[Sample] = {
    val sqrt = MathOperation(d => Math.sqrt(d))
    val s2 = Real(Metadata(names.last), sqrt(names.dropRight(1).map(name => sample.findVariableByName(name)
                 .getOrElse(throw new Exception("variable " + name + " not found."))).map(_ ** 2).reduce(_ + _)).getVariables.head.getData)
    Some(Sample(sample.domain, Tuple(sample.range.toSeq :+ s2)))
  }

}

object VectorMagnitudeDerivation {
  def apply(str: String): VectorMagnitudeDerivation = new VectorMagnitudeDerivation(str.split(" "))
}