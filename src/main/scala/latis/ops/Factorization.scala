package latis.ops

import latis.dm._

class Factorization extends Operation {

  private var factor: String = null

  //TODO: abstract up
  def apply(dataset: Dataset): Dataset = {
    //TODO: metadata, add provo
    Dataset(dataset.getVariables.map(op(_)))
  }
  
  private def op(v: Variable): Variable = v match {
    case f: Function => {
      /*
       * TODO: require Index Function, for now
       * ds: i -> (t,a)
       * factor must be a range var (scalar)
       * 
       * ds.factorOut("t")
       *   t -> i -> (a)
       * .reduce
       *   t -> a (if length of every nested function is 1)
       * 
       */
      
      
      ???
    }

    case _ => v
  }
}

object Factorization {
  
  //TODO: Dataset: def factorOut(factor: String): Dataset 
  def apply(factor: String) = {
    val f = new Factorization
    f.factor = factor
    f
  }
}