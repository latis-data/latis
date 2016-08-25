package latis.ops

import org.junit.{Test,Ignore}
import scala.math._
import org.junit.Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.writer.AsciiWriter
import latis.data.value.LongValue
import latis.data.value.DoubleValue

class TestSortingOperation {
  
  lazy val f1 = {
    val f = (p: (Int, Int)) => Tuple(Real(Metadata("longitude"), p._1), Real(Metadata("latitude"), p._2))
    val dom = Seq((0,0), (1, 0), (0,1), (1,1)).map(f)
    val ran = Seq(0,1,4,5).map(Real(Metadata("a"), _))
    Dataset(Function(dom.zip(ran).map(p => Sample(p._1, p._2))))
  }
  lazy val f2 = {
    val f = (p: (Int, Int)) => Tuple(Real(Metadata("longitude"), p._1), Real(Metadata("latitude"), p._2))
    val dom = Seq((0,0), (1, 0), (0,1), (1,1)).map(f)
    val ran = Seq(0,1,4,5).map(Real(Metadata("a"), _))
    Dataset(Function(dom.zip(ran).map(p => Sample(p._1, p._2))))
  }
   
  @Test
  def sort_a_datasets_function {
    val x = Sort()(f1)
    x match {
     case Dataset(Function(it)) => {
       val r1 = it.next match {
         case Sample(Tuple(t1),Real(t2)) => {
           assertEquals((0,0),(t1(0).getNumberData.doubleValue,t1(1).getNumberData.doubleValue))
           assertEquals(0.0,t2,0.0)
         }
       }
       val r2 = it.next match {
         case Sample(Tuple(t1),Real(t2)) => {
           assertEquals((0,1),(t1(0).getNumberData.doubleValue,t1(1).getNumberData.doubleValue))
           assertEquals(4.0,t2,0.0)
         }
       }
       val r3 = it.next match {
         case Sample(Tuple(t1),Real(t2)) => {
           assertEquals((1,0),(t1(0).getNumberData.doubleValue,t1(1).getNumberData.doubleValue))
           assertEquals(1.0,t2,0.0)
         }
       }
       val r4 = it.next match {
         case Sample(Tuple(t1),Real(t2)) => {
           assertEquals((1,1),(t1(0).getNumberData.doubleValue,t1(1).getNumberData.doubleValue))
           assertEquals(5.0,t2,0.0)
         }
       }
     }
   }
  }
   
  @Test 
  def can_compare_tuples_of_integers {
    val x = Seq(Integer(1),Integer(0))
    val res = VarSort(x)
    List(0,1).zip(res).map(f => {
      val num = f._2 match {
        case Integer(i) => i
      }
      assertEquals(f._1,num)
    })
  }
  
  @Test 
  def can_compare_tuples_of_Text {
    val x = Seq(Text("b"),Text("a"))
    val res = VarSort(x)
    List("a","b").zip(res).map(f => {
      val s = f._2 match {
        case Text(t) => t
      }
      assertEquals(f._1,s)
    })
  }
  
  @Test 
  def can_compare_tuples_of_Real {
    val x = Seq(Real(2),Real(1))
    val res = VarSort(x)
    List(1,2).zip(res).map(f => {
      val num = f._2 match {
        case Real(r) => r
      }
      assertEquals(f._1,num,0.0)
    })
  }
  
  @Test
  def can_compare_tuples_of_tuples_of_ints {
    val x = Seq(Tuple(Integer(1),Integer(0)),Tuple(Integer(0),Integer(1)))
    val res = VarSort(x)
    
    res.head match {
      case Tuple(t) => {
        val a = t.head
        val b = t.last
        assertEquals(LongValue(0),a.getData)
        assertEquals(LongValue(1),b.getData)
      }
    }
    res.last match {
      case Tuple(t) => {
        val a = t.head
        val b = t.last
        assertEquals(LongValue(1),a.getData)
        assertEquals(LongValue(0),b.getData)
      }
    }
  }
  
    @Test
  def can_compare_tuples_of_tuples_of_reals {
    val x = Seq(Tuple(Real(1),Real(0)),Tuple(Real(0),Real(1)))
    val res = VarSort(x)
    
    res.head match {
      case Tuple(t) => {
        val a = t.head
        val b = t.last
        assertEquals(DoubleValue(0),a.getData)
        assertEquals(DoubleValue(1),b.getData)
      }
    }
    res.last match {
      case Tuple(t) => {
        val a = t.head
        val b = t.last
        assertEquals(DoubleValue(1),a.getData)
        assertEquals(DoubleValue(0),b.getData)
      }
    }
  }
  
  @Test
  def can_compare_tuples_of_tuples_of_mixed {
    val x = Seq(Tuple(Real(1),Integer(0)),Tuple(Real(0),Integer(1)))
    val res = VarSort(x)
    res.head match {
      case Tuple(t) => {
        val a = t.head
        val b = t.last
        assertEquals(DoubleValue(0),a.getData)
        assertEquals(LongValue(1),b.getData)
      }
    }
    res.last match {
      case Tuple(t) => {
        val a = t.head
        val b = t.last
        assertEquals(DoubleValue(1),a.getData)
        assertEquals(LongValue(0),b.getData)
      }
    }
  }
  
  @Test(expected = classOf[Exception])
  def can_compare_tuples_of_tuples_of_different_lengths {
    val x = Seq(Tuple(Real(1),Integer(0)),Tuple(Real(0)))
    val res = VarSort(x)
  }
  
  @Test(expected = classOf[Exception])
  def can_compare_tuples_of_tuples_of_functions {
    val ff1 = f1 match {
      case Dataset(f: Function) => f
    }
    val ff2 = f2 match {
      case Dataset(f: Function) => f
    }
    val x = Seq(Tuple(ff1),Tuple(ff2))
    val res = VarSort(x)
  }
}
