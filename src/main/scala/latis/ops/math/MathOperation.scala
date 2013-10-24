package latis.ops.math

//import latis.math.BinOp
import latis.dm._
import latis.ops.xform._
//import latis.dm.implicits._

//class MathOperation(op: Variable => Variable) extends Morphism {
class BinaryMathOperation(op: (Double, Double) => Double, other: Dataset) extends Transformation {
  //TODO: take advantage of Transformation's transform(v) ?
  //TODO: should this be built with the 1st dataset then eval'd with other?
  
  override def apply(dataset: Dataset): Dataset = {
    //Expose the Variable "wrapped" in the Dataset monad. 
    //  Only an issue if Dataset has just one Var, otherwise we want to treat it as a Tuple which it IS
    //TODO: just add another case to operate: case (_, ds: Dataset) => operate(v1, ds.unwrap, op)?
    //TODO: maybe Dataset should not BE-A Variable?
    val otherVar: Variable = other.unwrap
    Dataset(dataset.getVariables.map(operate(_, otherVar, op)))
    //TODO: provenance metadata
  }
  
  //TODO: move to companion object? No, MorphedFunction uses "this"
  private def operate(v1: Variable, v2: Variable, op: (Double, Double) => Double): Variable = (v1, v2) match {
    //TODO: consider partial function, apply just one Var, get a unary operation
    
    case (t1: Text, t2: Text) => throw new UnsupportedOperationException("Can't do math with Text.")
    case (t: Text, n: Number) => Real(Double.NaN)
    case (n: Number, t: Text) => Real(Double.NaN)
    
    case (n @ Number(d1), Number(d2)) => Real(op(d1, d2))
    //TODO: allow Integers to remain Integers...?
    
    case (n: Number, t @ Tuple(vars)) => Tuple(vars.map(operate(n, _, op)))
    case (t @ Tuple(vars), n: Number) => Tuple(vars.map(operate(_, n, op)))
    
    case (t @ Tuple(vars1), Tuple(vars2)) => {
      //require that they have the same number of elements
      //TODO: flatten Tuple of one? namespace concerns
//TODO: now that we are operating only on the Dataset monad, need to flatten
      if (vars1.length != vars2.length) {
        val msg = "Operation requires Tuples with the same number of elements."
        throw new UnsupportedOperationException(msg)
      }
      Tuple((vars1, vars2).zipped.map(operate(_, _, op)))
    }
    
    //TODO: ensure that the domains are equal, resample second Function to the domain of the first
    //case (f1 @ Function(d1, r1), f2 @ Function(d2, r2)) => {
    case (f: Function, v: Variable) => {
      //TODO: if no data in Function: Function(d1, (r1, r2).zipped.map(operate(_,_,op)))
      //  let's just assume the iterator will take care of getting the data
      TransformedFunction(f, this) 
      //TODO: do we need a MorphedFunction? should the new Dataset encapsulate the morphism?
      //  but Function can't ask parent Dataset if there is a morphism to apply
      //  what about a MorphedScalar? simply laziness where we need it?
    }
//    case (f1 @ Function(d1, r1), f2: Function) => {
//      val f3 = f2.resample(d1) //no-op if domains are the same
//      val r3 = f3.range
//      Function(d1, (r1, r3).zipped.map(binOp(_,_,op))) 
//    }
    
    //TODO: use builder to get appropriate subclass
    /*
     * TODO: use wrapper, be lazy
     * pass unary op, partially applied binOp?
     * but if 2nd arg is a complex Var, we will need to come back here
     * Sample? Function of length 1?
     *   no need, math doesn't need domain value
     * or make new Function with new data object?
     *   but data might not have enough context to support lazy math?
     * just delegate back here with the range samples?
     * where to deal with resampling?
     *   another wrapper for the resampled Function?
     *   always do it even if not needed so we don't have to read data and check early
     *   
     * 
     */
    
//    //TODO: If Function does not have the data, keep iterating
//    case (f @ Function(d, r), v2: Variable) => {
//      if (f.data.isEmpty) Function(d, operate(r, v2, op))
//      else MorphedFunction(f, this) //allows us to be lazy, morph by sample
//    }
//    case (v1: Variable, f @ Function(d, r)) => {
//      if (f.data.notEmpty) Function(d, operate(v1, r, op))
//      else MorphedFunction(f, MathOperation(op, v1)) //allows us to be lazy, morph by sample
///*
// * TODO: need to support reverse order
// * pre/post, left/right subclasses?
// * can we still use this?
// * op.reverse?
// * maybe we shouldn't allow this?
// *   require Function to be first arg of binary op?
// *   -f + v instead of v - f
// *   recip(f) * v instead of v/f
// */
//    }

    case _ => throw new UnsupportedOperationException("Can't operate with these two Variables: "+v1+", "+v2) 
  }
  
}
//  override def binOp(variable: Variable): Variable = (variable, other) match {
//    case (t1: Text, t2: Text) => throw new UnsupportedOperationException("Can't do math with Text.")
//    case (t: Text, n: Number) => Real(Double.NaN)
//    case (n: Number, t: Text) => Real(Double.NaN)
//    
//    case (n @ Number(d1), Number(d2)) => Real(op(d1,d2))
//    //TODO: allow Integers to remain Integers...?
//    
//    case (n: Number, t @ Tuple(vars)) => Tuple(vars.map(MathOperation(op, _).morphVariable))
//    /*
//     * can't recurse with this
//     * need to make new MathOperation?
//     * seems a bit much, but may allow better metadata handling?
//     *   need to munge units md
//     *   can't do that at the Dataset level, needs to happen within recursion
//     *   but we are self-contained here
//     *   morphMetadata doesn't have to do it all
//     *   start with adding provenance
//     * what about wrapping each in its own Dataset? definitely overkill
//     * +we are in the innerds of a monadic context here
//     * do we really need to wrap each iteration in a new Morphism?
//     * beyond the initial morph(dataset), probably not
//     * might as well make out own convenient recursive method instead of trying to force "morph" to work
//     * there should be no other external entry point than morph(dataset)
//     * 
//     * delegate to external math class or do it all here?
//     * is there another use for math?
//     * but it sent us here
//     * still need a place to define +,-,*,/...
//     * it Variable mixin the best?
//     *   seems to work
//     * 
//     */
//    
//    case (t @ Tuple(vars), n: Number) => Tuple(vars.map(_.binOp(op, n)))
//    
//    case (t @ Tuple(vars1), Tuple(vars2)) => {
//      //require that they have the same number of elements
//      //TODO: flatten Tuple of one? namespace concerns
//      if (vars1.length != vars2.length) {
//        val msg = "Operation requires Tuples with the same number of elements."
//        throw new UnsupportedOperationException(msg)
//      }
//      Tuple((vars1, vars2).zipped.map(_.binOp(op, _)))
//    }
//    //case s: Scalar => morphScalar(s)
//    //case Tuple(vars)  => Some(Tuple(vars.flatMap(morphVariable(_))))
//    //case f: Function  => Some(MorphedFunction(f, this))
//  }
//  
//  override def morphSample(sample: (Variable, Variable)) = {
//    val domain = sample._1
//    val range = sample._2
//    Some((domain, op(range)))
//  }
//}

//class PreBinaryOp(op: BinOp, that: Variable) extends MathOperation {
//  // this op that
//  
//  override def morphSample(sample: (Variable, Variable)) = {
//    val domain = sample._1
//    val range = sample._2
//    Some((domain, range.binOp(op, that)))
//  }
//  
////  def morphVariable(v: Variable): Variable = v match {
////    case s: Scalar => 
////  }
//}
//
//class PostBinaryOp(op: BinOp, that: Variable) extends MathOperation {
//  //that op this
//  
//  override def morphSample(sample: (Variable, Variable)) = {
//    val domain = sample._1
//    val range = sample._2
//    Some((domain, that.binOp(op, range)))
//  }
//}

//class UnaryOp(op: UnOp) extends MathOperation

object MathOperation {
  
  def apply(op: (Double,Double) => Double, ds: Dataset) = new BinaryMathOperation(op, ds)
  //def apply(v: Variable, op: BinOp) = new PostBinaryOp(op, v)
  //def apply(op: BinOp, v: Variable) = new PreBinaryOp(op, v)
}