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
import latis.ops.agg.FullOuterJoin2

class TestFullOuterJoin2 {
  
  val mdt = Metadata("name" -> "t")
  val mda = Metadata("name" -> "a", "missing_value" -> "0")
  val mdb = Metadata("name" -> "b", "missing_value" -> "0")
  val mdc = Metadata("name" -> "c", "missing_value" -> "0")
  val mdd = Metadata("name" -> "d", "missing_value" -> "0")
  
  @Test
  def same_in_both = {
    /*
     * a t1 t2  b
     * 1  1  1  2
     * 2  2  2  4
     * 3  3  3  6
     */
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1")) //.force
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2")) //.force
    
    val op = new FullOuterJoin2()
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
    /*
     * a t1 t2  b
     * 1  1  1  2
     *       2  4
     * 3  3  3  6
     */
    val samples1 = List(1,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,0,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_second_at_end = {
    /*
     * a t1 t2  b
     * 1  1  1  2
     * 2  2  2  4
     *       3  6
     */
    val samples1 = List(1,2).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,0,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_second_at_start = {
    /*
     * a t1 t2  b
     *       1  2
     * 2  2  2  4
     * 3  3  3  6
     */
    val samples1 = List(2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,0,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_first_in_middle = {
    /*
     * a t1 t2  b
     * 1  1  1  2
     * 2  2 
     * 3  3  3  6
     */
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,0), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_first_at_end = {
    /*
     * a t1 t2  b
     * 1  1  1  2
     * 2  2  2  4
     * 3  3 
     */
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,0), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def extra_in_first_at_start = {
    /*
     * a t1 t2  b
     * 1  1
     * 2  2  2  4
     * 3  3  3  6
     */
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(2,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,0), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def empty_second = {
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List[Sample]()
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(Real(mdt), Real(mdb), Iterator.empty, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,1,0), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,2,0), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,3,0), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def empty_first = {
    val samples1 = List[Sample]()
    val samples2 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(Real(mdt), Real(mda), Iterator.empty, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((1,0,2), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((2,0,4), (t,a,b))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b)))) => assertEquals((3,0,6), (t,a,b))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test(expected=classOf[UnsupportedOperationException])
  def mismatched_domain = {
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(Metadata("z"), i), Real(mdb, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
  }
  
  @Test
  def tuple_codomains = {
    val samples1 = List(2,3).map(i => Sample(Real(mdt, i), Tuple(Real(mda, i), Real(mdc, i+2))))
    val samples2 = List(1,3).map(i => Sample(Real(mdt, i), Tuple(Real(mdb, i*2), Real(mdd, i*3))))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b), Real(c), Real(d)))) => assertEquals((1,0,0,2,3), (t,a,b,c,d))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b), Real(c), Real(d)))) => assertEquals((2,2,4,0,0), (t,a,b,c,d))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b), Real(c), Real(d)))) => assertEquals((3,3,5,6,9), (t,a,b,c,d))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def three_datasets = {
    val samples1 = List(1,2).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,3).map(i => Sample(Real(mdt, i), Real(mdb, i*2)))
    val samples3 = List(2,3).map(i => Sample(Real(mdt, i), Real(mdc, i+2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1"))
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2"))
    val ds3 = Dataset(Function(samples3, Metadata("function3")), Metadata("dataset3"))
    val op = new FullOuterJoin2()
    val ds = op(Seq(ds1, ds2, ds3))
    ds match {
      case Dataset(Function(it)) => {
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b), Real(c)))) => assertEquals((1,1,2,0), (t,a,b,c))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b), Real(c)))) => assertEquals((2,2,0,4), (t,a,b,c))}
        it.next match {case Sample(Real(t), Tuple(Seq(Real(a), Real(b), Real(c)))) => assertEquals((3,0,6,5), (t,a,b,c))}
        assert(!it.hasNext)
      }
    }
  }
  
  @Test @Ignore //TODO: deal with duplicate names in join
  def duplicate_codomain_variable = {
    val samples1 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i)))
    val samples2 = List(1,2,3).map(i => Sample(Real(mdt, i), Real(mda, i*2)))
    val ds1 = Dataset(Function(samples1, Metadata("function1")), Metadata("dataset1")) //.force
    val ds2 = Dataset(Function(samples2, Metadata("function2")), Metadata("dataset2")) //.force
    
    val op = new FullOuterJoin2()
    val ds = op(ds1, ds2)
    latis.writer.AsciiWriter.write(ds)
  }
}