package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata
import latis.ops.filter.Selection

class TestSelection {
  
  implicit def stringToMetadata(name: String): Metadata = Metadata(name)
  
  // time -> value, column oriented data
  lazy val scalarFunction: Function = {
    val domain = Real("time", Seq(1.0,2.0,3.0,4.0,5.0))
    val range = Real("value", Seq(1.0,2.0,3.0,4.0,3.0))
    Function(domain, range)
  }
  
  //TODO: run same tests against multiple Functions
  def testSelection(f: Function, s: String, expected: Function) {
    val filter = Selection(s)
    val ds = filter(f)
    //AsciiWriter().write(expected)
    //AsciiWriter().write(ds)
    assert(expected equals ds(0))
  }
  
  
  //TODO: test more than length
  
  @Test
  def select_equal_value {
    val expected = Function(Real("time", Seq(3.0,5.0)), Real("value", Seq(3.0,3.0)))
    testSelection(scalarFunction, "value=3", expected)
  }
  
  @Test
  def select_gt_value {
    val filter = Selection("value>2")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(3, n)
  }
  
  @Test
  def select_lt_value {
    val filter = Selection("value<3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(2, n)
  }
  
  @Test
  def select_ge_value {
    val filter = Selection("value>=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(3, n)
  }
  
  @Test
  def select_le_value {
    val filter = Selection("value<=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(4, n)
  }
  
  @Test
  def select_ne_value {
    val filter = Selection("value!=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(3, n)
  }
  
  
  @Test
  def select_equal_time {
    val filter = Selection("time=3")
    val ds = filter(scalarFunction)
    val n = ds.length
    assertEquals(1, n)
    //AsciiWriter().write(ds)
  }
  
  //@Test
  def test_data_from_file {
    val reader = TsmlReader("datasets/test/scalar.tsml")
    val ds0 = reader.getDataset
    //println(ds)
    //val filter = SelectionFilter("foo>=5")
    val filter = Selection("ssn>=15")
    val ds = filter(ds0)
    //AsciiWriter().write(ds)
    //NOTE: iterable once issues
    
    val f = ds(0).asInstanceOf[Function]
    val n = f.length
    assertEquals(5, n)
  }
  
  //TODO: iterable once issues
  //TODO: Text data
}




