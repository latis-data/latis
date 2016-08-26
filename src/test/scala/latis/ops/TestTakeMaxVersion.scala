package latis.ops

import latis.dm._
import latis.metadata._
import org.junit._
import Assert._

class TestTakeMaxVersion {
  
  @Test
  def two_versions = {
    val md = Metadata(Map("name" -> "time", "units" -> "milliseconds since 1970-01-01"))
    
    val sample1 = Sample(Integer(Metadata("i"), 1), Tuple(Integer(md, 0), Integer(Metadata("version"), 1), Text(Metadata("foo"), "foo_t0_v1") ) )
    val sample2 = Sample(Integer(Metadata("i"), 2), Tuple(Integer(md, 0), Integer(Metadata("version"), 2), Text(Metadata("foo"), "foo_t0_v2") ) )
    val sample3 = Sample(Integer(Metadata("i"), 3), Tuple(Integer(md, 1), Integer(Metadata("version"), 2), Text(Metadata("foo"), "foo_t1_v2") ) )
    val sample4 = Sample(Integer(Metadata("i"), 4), Tuple(Integer(md, 1), Integer(Metadata("version"), 3), Text(Metadata("foo"), "foo_t1_v3") ) )
    
    val samples = List(sample1, sample2, sample3, sample4)
    
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = TakeMaxVersion()(ds1)
    
    //latis.writer.AsciiWriter.write(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((0, "foo_t0_v2"), (i, txt))
        }
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((1, "foo_t1_v3"), (i, txt))
        }
      }
      assert(!it.hasNext)
    }
  }
  
  @Test
  def three_versions_not_in_order = {
    val md = Metadata(Map("name" -> "time", "units" -> "milliseconds since 1970-01-01"))
    
    val sample1 = Sample(Integer(Metadata("i"), 1), Tuple(Integer(md, 0), Integer(Metadata("version"), 1), Text(Metadata("foo"), "foo_t0_v1") ) )
    val sample2 = Sample(Integer(Metadata("i"), 2), Tuple(Integer(md, 0), Integer(Metadata("version"), 2), Text(Metadata("foo"), "foo_t0_v2") ) )
    val sample3 = Sample(Integer(Metadata("i"), 3), Tuple(Integer(md, 0), Integer(Metadata("version"), 4), Text(Metadata("foo"), "foo_t0_v4") ) )
    val sample4 = Sample(Integer(Metadata("i"), 4), Tuple(Integer(md, 1), Integer(Metadata("version"), 2), Text(Metadata("foo"), "foo_t1_v2") ) )
    val sample5 = Sample(Integer(Metadata("i"), 5), Tuple(Integer(md, 1), Integer(Metadata("version"), 3), Text(Metadata("foo"), "foo_t1_v3") ) )
    val sample6 = Sample(Integer(Metadata("i"), 6), Tuple(Integer(md, 1), Integer(Metadata("version"), 1), Text(Metadata("foo"), "foo_t1_v1") ) )
    
    val samples = List(sample1, sample2, sample3, sample4, sample5, sample6)
    
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = TakeMaxVersion()(ds1)
    
    //latis.writer.AsciiWriter.write(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((0, "foo_t0_v4"), (i, txt))
        }
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((1, "foo_t1_v3"), (i, txt))
        }
      }
      assert(!it.hasNext)
    }
  }
  
  @Test
  def same_versions = {
    val md = Metadata(Map("name" -> "time", "units" -> "milliseconds since 1970-01-01"))
    
    val sample1 = Sample(Integer(Metadata("i"), 1), Tuple(Integer(md, 0), Integer(Metadata("version"), 1), Text(Metadata("foo"), "foo_t0_v1") ) )
    val sample2 = Sample(Integer(Metadata("i"), 2), Tuple(Integer(md, 0), Integer(Metadata("version"), 1), Text(Metadata("foo"), "foo_t0_v1") ) )
    val sample3 = Sample(Integer(Metadata("i"), 3), Tuple(Integer(md, 1), Integer(Metadata("version"), 5), Text(Metadata("foo"), "foo_t1_v5") ) )
    val sample4 = Sample(Integer(Metadata("i"), 4), Tuple(Integer(md, 1), Integer(Metadata("version"), 5), Text(Metadata("foo"), "foo_t1_v5") ) )
    
    val samples = List(sample1, sample2, sample3, sample4)
    
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = TakeMaxVersion()(ds1)
    
    //latis.writer.AsciiWriter.write(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((0, "foo_t0_v1"), (i, txt))
        }
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((1, "foo_t1_v5"), (i, txt))
        }
      }
      assert(!it.hasNext)
    }
  }
  
  @Test
  def one_version = {
    val md = Metadata(Map("name" -> "time", "units" -> "milliseconds since 1970-01-01"))
    
    val sample1 = Sample(Integer(Metadata("i"), 1), Tuple(Integer(md, 0), Integer(Metadata("version"), 1), Text(Metadata("foo"), "foo_t0_v1") ) )
    val sample2 = Sample(Integer(Metadata("i"), 4), Tuple(Integer(md, 1), Integer(Metadata("version"), 5), Text(Metadata("foo"), "foo_t1_v5") ) )
    
    val samples = List(sample1, sample2)
    
    val ds1 = Dataset(Function(samples, Metadata("function")), Metadata("dataset"))
    val ds2 = TakeMaxVersion()(ds1)
    
    //latis.writer.AsciiWriter.write(ds1)
    //latis.writer.AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((0, "foo_t0_v1"), (i, txt))
        }
        it.next match {
          case Sample(Integer(i), Tuple(Seq(Text(txt)))) => assertEquals((1, "foo_t1_v5"), (i, txt))
        }
      }
      assert(!it.hasNext)
    }
  }
  
}
