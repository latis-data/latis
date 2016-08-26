package latis.ops

import org.junit._
import Assert._
import latis.ops.agg._
import latis.dm._
import latis.metadata._

class TestCollection {

  val op = Collection()
  val func1 = Function(Seq(Integer(1)), Metadata("f1"))
  val func2 = Function(Seq(Integer(2)), Metadata("f2"))
  val ds_func1 = Dataset(func1)
  val ds_func2 = Dataset(func2)
  val ds_named1 = Dataset(Tuple(Seq(func1, func2), Metadata("t1")))
  val ds_named2 = Dataset(Tuple(Seq(func1, func2), Metadata("t2")))
  val ds_unnamed1 = Dataset(Tuple(func1, func2)) 
  val ds_unnamed2 = Dataset(Tuple(func1, func2)) 
 
  //Check the structure of the dataset using pattern matching
  //then check data preservation
  @Test
  def TestFunctionCollect{
    op(ds_func1, ds_func2) match {
      case Dataset(Tuple(Seq(f1: Function, f2: Function))) => {
        assertEquals("f1", f1.getName)
        assertEquals("f2", f2.getName)
      }
      case _ => fail()
    }
  }

  @Test
  def TestFunctionWithNamedTuple{
    op(ds_func1, ds_named1) match {
      case Dataset(Tuple(Seq(f1: Function, t @ Tuple(Seq(f2, f3))))) => {
        assertEquals("t1", t.getName)
        assertEquals("f1", f1.getName)
        assertEquals("f1", f2.getName)
        assertEquals("f2", f3.getName)
      }
      case _ => fail()
    }

    op(ds_named1, ds_func1) match {
      case Dataset(Tuple(Seq(t @ Tuple(Seq(f1, f2)), f3: Function))) => {
        assertEquals("t1", t.getName)
        assertEquals("f1", f1.getName)
        assertEquals("f2", f2.getName)
        assertEquals("f1", f3.getName)
      }
      case _ => fail()
    }
  }

  @Test
  def TestFunctionWithUnamedTuple{
    op(ds_func1, ds_unnamed1) match {
      case Dataset(Tuple(Seq(f1: Function, f2: Function, f3: Function))) => {
        assertEquals("f1", f1.getName)
        assertEquals("f1", f2.getName)
        assertEquals("f2", f3.getName)
      }
      case _ => fail()
    }

    op(ds_unnamed1, ds_func1) match {
      case Dataset(Tuple(Seq(f1: Function, f2: Function, f3: Function))) => {
        assertEquals("f1", f1.getName)
        assertEquals("f1", f3.getName)
        assertEquals("f2", f2.getName)
      }
      case _ => fail()
    }
  }

  @Test
  def TestNamedTuples{
    op(ds_named1, ds_named2) match {
      case Dataset(Tuple(Seq(t1 @ Tuple(_), t2 @ Tuple(_)))) => {
        assertEquals("t1", t1.getName)
        assertEquals("t2", t2.getName)
      }
      case _ => fail()
    }
  }

  @Test
  def TestMixedTuples{
    op(ds_named1, ds_unnamed1) match {
      case Dataset(Tuple(Seq(t1 @ Tuple(_), t2 @ Tuple(_)))) => {
        assertEquals("t1", t1.getName)
        assertEquals("unknown", t2.getName)
      }
      case _ => fail()
    }

    op (ds_unnamed1, ds_named1) match {
      case Dataset(Tuple(Seq(t1 @ Tuple(_), t2 @ Tuple(_)))) => {
        assertEquals("unknown", t1.getName)
        assertEquals("t1", t2.getName)
      }
      case _ => fail()
    }
  }

  @Test
  def TestUnnamedTuples{
    op(ds_unnamed1, ds_unnamed2) match {
      case Dataset(Tuple(Seq(f1: Function, f2: Function, f3: Function, f4: Function))) => 
      case _ => fail()
    }
  }
}
