package latis.ops.math

import BinOp._
import latis.dm._

trait BasicMath { this: Dataset =>
  //TODO: ComplexMath (Complex extends Scalar[(Float,Float)]), VectorMath (Vector extends Tuple), MatrixMath?
  //TODO: think about how we can do lazy Math, without sucking in all the data: iterator
  //  support foreach so we can do for comprehensions: for (d <- data)
  //TODO: wrap result in a new Dataset
  //TODO: add to provenance metadata
 
/*
 * TODO: 2013-06-06 
 * should these be defined in terms of Dataset? no Variable stands alone
 * Dataset is the monad
 * define all ops on Dataset (e.g. iterator), new Vars wrapped in a new Dataset
 * won't be a problem of Function + Real returning Variable when we really want a Function
 * should each sample of the iterator be a Dataset?
 *   implicit varToDataset?
 *   do it the monadic way, "lift"?
 *   but would hate to make a new Dataset object for each sample
 * where do we draw the line of being safely within the context of Dataset so we can expose its inerds?
 *   iterating seems to be a fair place to expose samples
 *   maybe even by definition, iterator/foreach exposes elements in a collection
 *   consider a List
 *   
 * 
 */
  
  //Define binary operation symbols that can be used on any Variable that extends this trait.
  def + (that: Dataset): Dataset  = MathOperation(ADD, that)(this) //this.binOp(ADD, that)
  def - (that: Dataset): Dataset  = MathOperation(SUBTRACT, that)(this) //this.binOp(SUBTRACT, that)
  def * (that: Dataset): Dataset  = MathOperation(MULTIPLY, that)(this) //this.binOp(MULTIPLY, that)
  def / (that: Dataset): Dataset  = MathOperation(DIVIDE, that)(this) //this.binOp(DIVIDE, that)
  def % (that: Dataset): Dataset  = MathOperation(MODULO, that)(this) //this.binOp(MODULO, that)
  def ** (that: Dataset): Dataset = MathOperation(POWER, that)(this) //this.binOp(POWER, that) //WARNING: has same operator precedence as multiplication
  
//  //def binOp(d1: Variable, d2: Variable, op: BinOp): Variable = (d1, d2) match {
//  def binOp(op: BinOp, that: Variable): Variable = (this, that) match {
//    //TODO: consider partial function, apply just one Var, get a unary operation
//    
//    case (t1: Text, t2: Text) => throw new UnsupportedOperationException("Can't do math with Text.")
//    case (t: Text, n: Number) => Real(Double.NaN)
//    case (n: Number, t: Text) => Real(Double.NaN)
//    
//    case (n @ Number(v1), Number(v2)) => Real(op(v1, v2))
//    //TODO: allow Integers to remain Integers...?
//    
//    case (n: Number, t @ Tuple(vars)) => Tuple(vars.map(n.binOp(op,_)))
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
//    
////    //TODO: ensure that the domains are equal, resample second Function to the domain of the first
////    case (f @ Function(d1, r1), Function(d2, r2)) => f.build(d1, (r1, r2).zipped.map(binOp(_,_,op)))
//////    case (f1 @ Function(d1, r1), f2: Function) => {
//////      val f3 = f2.resample(d1) //no-op if domains are the same
//////      val r3 = f3.range
//////      Function(d1, (r1, r3).zipped.map(binOp(_,_,op))) 
//////    }
////    
//    //TODO: use builder to get appropriate subclass
//    /*
//     * TODO: use wrapper, be lazy
//     * pass unary op, partially applied binOp?
//     * but if 2nd arg is a complex Var, we will need to come back here
//     * Sample? Function of length 1?
//     *   no need, math doesn't need domain value
//     * or make new Function with new data object?
//     *   but data might not have enough context to support lazy math?
//     * just delegate back here with the range samples?
//     * where to deal with resampling?
//     *   another wrapper for the resampled Function?
//     *   always do it even if not needed so we don't have to read data and check early
//     * MathOperation extends Morphism?
//     *   
//     */
////    case (f @ Function(d, r), v2: Variable) => MorphedFunction(f, MathOperation((v: Variable) => binOp(op, v2)))
////      //Function(d, r.map(binOp(_, v2, op)))
////    case (v1: Variable, f @ Function(d, r)) => MorphedFunction(f, MathOperation((v: Variable) => v1.binOp(op, v)))
////      //f.build(d, r.map(binOp(v1, _, op)))
//
//    case _ => throw new UnsupportedOperationException("Can't operate with these two Variables: "+this+", "+that) 
//  }
  
}

object BasicMath {
  
  //TODO: use Morphism
//  def abs(number: Number): Dataset = {
//    val v = number.toDouble
//    Real(Math.abs(v)) 
//  }
}