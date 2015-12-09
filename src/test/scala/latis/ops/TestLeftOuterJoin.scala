package latis.ops

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer._
import latis.ops.agg.CollectionAggregation
import latis.ops.agg.TileAggregation
import latis.dm._
import latis.ops.agg.Intersection
import latis.metadata.Metadata
import latis.ops.agg.LeftOuterJoin

class TestLeftOuterJoin {
  
  @Test
  def same_in_both = {
    val samples1 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_second_in_middle = {
    val samples1 = List(1,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_second_at_end = {
    val samples1 = List(1,2).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_second_at_start = {
    val samples1 = List(2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_first_in_middle = {
    val samples1 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(1,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2), (t,a)); assert(b.isNaN())}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_first_at_end = {
    val samples1 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(1,2).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3), (t,a)); assert(b.isNaN())}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_first_at_start = {
    val samples1 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List(2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1), (t,a)); assert(b.isNaN())}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def empty_second = {
    val samples1 = List(1,2,3).map(i => Sample(Real(Metadata("t"), i), Real(Metadata("a"), i)))
    val samples2 = List[Sample]() //.map(i => Sample(Real(Metadata("t"), i), Real(Metadata("b"), i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new LeftOuterJoin()
    val ds = op(ds1, ds2)
    AsciiWriter.write(ds)
  }
}