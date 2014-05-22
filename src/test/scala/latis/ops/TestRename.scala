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

}




