package latis.ops

import latis.dm._

class Factorization extends Operation {

  //TODO: constructor arg?
  private var factor: String = null

  //TODO: impl in terms of applyToVariable...
  override def apply(dataset: Dataset): Dataset = {
    //TODO: metadata, add provo
    Dataset(dataset.getVariables.map(op(_)))
  }
  
  private def op(variable: Variable): Variable = variable match {
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
       * 
       */
      if (! f.getDomain.isInstanceOf[Index]) ???
      
      f.getRange match {
        case t: Tuple => {
          
        }
      }
      
      ???
    }

    case Sample(d,r) => {
      ???
    }
    
    /*
     * factor var out of tuple
     * TODO: handle interleaved Data
     * (a,b,c) factorOut("b") => (b,(a,c))
     * does returning a Sample make sense?
     */
    case Tuple(vars) => {
      val (dvars, rvars) = vars.partition(_.hasName(factor))
      if (dvars.length == 0) throw new Error("Factor not found in Tuple.")
      if (dvars.length > 1) throw new Error("Factor found more than once in Tuple.")
      val dvar = dvars.head
      
      rvars.length match {
        case 0 => variable //only had one element? no change, Sample with Index?
        case 1 => Tuple(dvar, rvars.head)
        case _ => Tuple(dvar, Tuple(rvars))
      }
    }
    
    case _ => ???
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