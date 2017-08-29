package latis.dm

import org.junit._
import org.junit.Assert._
import latis.metadata.Metadata
import scala.collection.mutable.Stack


class TestModel {
  
//  val model = {
//    val a = ScalarType("A")
//    val b = ScalarType("B")
//    val x = ScalarType("X")
//    val v = FunctionType(x, TupleType(Seq(a,b), "foo"))
//    Model(v)
//  }
//  
//  @Test
//  def print = {
//    println(model)
//  }
//  
//  def traverse(model: Model): Unit = {
//    def go(v: VariableType): Unit = {
//      //recurse
//      v match {
//        case _: ScalarType     => //end of this branch
//        case TupleType(vars,_,_)   => vars.map(go(_))
//        case FunctionType(d,c,_,_) => go(d); go(c)
//      }
//      println(v)
//    }
//    go(model.variable)
//  }
//  
//  type V = VariableType
//  type S = ScalarType
//  type T = TupleType
//  type F = FunctionType
//  
//  def fromSeq(vars: Seq[Option[V]]): Model = {
//    def go(vs: Seq[Option[V]], hold: Stack[Option[V]]): Option[V] = {
//      vs.headOption match {
//        case Some(s: Option[S]) => go(vs.tail, hold :+ s)
//        case Some(ot: Option[T])  => ot match {
//          case Some(t) =>
//            val n1 = t.variables.length
//            val tvs = hold.take(n1).flatten //drop Nones
//            val n2 = tvs.length
//            val newT = if (n2 == 0) None
//              else if (n2 == 1) Option(tvs.head)
//              else Option(TupleType(tvs))
//            go(vs.tail, hold.drop(n1).push(newT))
//          case None => ot
//        }
//        case Some(_) => ???
//        case None => hold.pop //done
//      }
//      
//      ???
//    }
//    
//    go(vars, Stack[Option[V]]()) match {
//      case Some(v) => Model(v)
//      //case None    => Model() //TODO: empty model
//    }
//  }
  
//  @Test
//  def traversal = {
//    traverse(model)
//  }
  
}