package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata

class TestRename {

  @Test
  def rename_dataset {
    val ds = Dataset(Real(Metadata("a")), Metadata("foo"))
    val ds2 = ds.rename("foo", "bar")
    //println(ds2)
    assertEquals("bar", ds2.getName)
  }

  @Test
  def rename_domain {
    val ds = TestDataset.time_series
    val op = Operation("rename", List("myTime","yourTime"))
    val ds2 = op(ds)
    val data = ds2.toStringMap
    assert(data.contains("yourTime"))
    assert(!data.contains("myTime"))
    assertEquals("1970/01/01", data("yourTime").head)
  }

  @Test
  def rename_range {
    val ds = TestDataset.time_series
    val op = Operation("rename", List("myReal","yourReal"))
    val ds2 = op(ds)
    val data = ds2.toStringMap
    assert(data.contains("yourReal"))
    assert(!data.contains("myReal"))
    assertEquals("1.1", data("yourReal").head)
  }

}




