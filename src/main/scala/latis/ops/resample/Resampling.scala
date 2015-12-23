package latis.ops.resample

import latis.dm.Sample
import latis.dm.Variable
import latis.ops.Operation
import latis.dm.Function

class Resampling(domainSet: Iterable[Variable]) extends Operation with NoInterpolation {
  /*
   * TODO: construct with DomainSet?
   * currently DomainSet is just Data
   * need to be able to match type, name?
   *   convert units...
   *   add metadata "template" to it?
   *   def apply(index: Int): Data
   *   def indexOf(data: Data): Int
   * needs to be iterable
   * 
   *   
   */
   

  override def applyToFunction(function: Function): Option[Variable] = {
    
    ???
  }

  def resample(samples: Array[Sample], domain: Variable): Some[Sample] = {
    //TODO: make sure domains are consistent, unit conversion...
    domain match {
      case _ => ???
    }

    ???
  }

}
