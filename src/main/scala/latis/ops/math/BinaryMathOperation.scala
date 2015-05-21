package latis.ops.math

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Number
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable

class BinaryMathOperation(op: (Double, Double) => Double, other: Dataset) extends MathOperation {
  //TODO: consider integer math, complex math
  
  /**
   * The second operand, encapsulated within the 'other' Dataset.
   */
  val otherVar: Variable = other.unwrap
  
  /**
   * Because we have the other Variable managed by this Operation and we will need to recurse on it
   * we can't use the regular Operation recursion (without making new Operations along the way).
   * Delegate to WrappedFunction for top level Functions but manage recursion here with 'operate'.
   */
  override def applyToVariable(variable: Variable): Option[Variable] = variable match {
    case f: Function => applyToFunction(f)
    case _ => Some(operate(variable, otherVar, op))
  }
    
  /**
   * Apply the operation 'op' to the given pair of Variables recursively.
   */
  private def operate(v1: Variable, v2: Variable, op: (Double, Double) => Double): Variable = (v1, v2) match {
    //TODO: use builder to get appropriate subclass
    //TODO: metadata
    
    //--- Scalar Math ---------------------------------------------------------
    
    case (t1: Text, t2: Text) => throw new UnsupportedOperationException("Can't do math with Text.")
    case (t: Text, n: Number) => Real(Double.NaN)
    case (n: Number, t: Text) => Real(Double.NaN)
    
    case (n1 @ Number(d1), n2 @ Number(d2)) => if(n1.getMetadata != n2.getMetadata) Real(op(d1, d2))
                                               else Real(n1.getMetadata, op(d1, d2)) 
    
    //--- Sample Math ---------------------------------------------------------
    
    case (Sample(d,r), v: Variable) => Sample(d, operate(r, v, op))
    case (v: Variable, Sample(d,r)) => Sample(d, operate(v, r, op))
    
    case (Sample(s @ Scalar(d1),r1), Sample(Scalar(d2),r2)) => {
      //TODO: assume 1D domains for now
      if (d1 != d2) throw new UnsupportedOperationException("BinaryMathOperation requires Samples to have the same domain value.")
      Sample(s, operate(r1, r2, op))
    }
    
    //--- Tuple Math ----------------------------------------------------------
    
    case (n: Number, Tuple(vars)) => Tuple(vars.map(operate(n, _, op)))
    case (Tuple(vars), n: Number) => Tuple(vars.map(operate(_, n, op)))
    
    case (Tuple(vars1), Tuple(vars2)) => {
      //require that they have the same number of elements
      //TODO: reduce Tuple of one? namespace concerns
      if (vars1.length != vars2.length) {
        val msg = "BinaryMathOperation requires that Tuples have the same number of elements."
        throw new UnsupportedOperationException(msg)
      }
      Tuple((vars1, vars2).zipped.map(operate(_, _, op)))
    }
    
    //--- Function Math -------------------------------------------------------
        
    case (f1: Function, f2: Function) => {
      //TODO: ensure that the domains are equal, resample second Function to the domain of the first
      //Let the sample math complain for now
      val it = (f1.iterator zip f2.iterator).map(p => operate(p._1, p._2, op).asInstanceOf[Sample])
      Function(f1.getDomain, f1.getRange, it)
    }
    
    case (f: Function, v: Variable) => {
      val it = f.iterator.map(smp => Sample(smp.domain, operate(smp.range, v, op)))
      Function(f.getDomain, f.getRange, it)
    }
    
    case (v: Variable, f: Function) => {
      val it = f.iterator.map(smp => Sample(smp.domain, operate(v, smp.range, op)))
      Function(f.getDomain, f.getRange, it)
    }
    
    //Error for anything else that slipped through the cracks
    case _ => throw new UnsupportedOperationException("Can't operate with these two Variables: "+v1+", "+v2) 
  }
  
}
