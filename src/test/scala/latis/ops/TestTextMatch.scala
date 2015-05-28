package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.data.SampledData
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.TestDataset
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata
import latis.ops.filter.NearestNeighborFilter
import latis.ops.filter.Selection
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestTextMatch {

  /*
   * TestDataset.function_of_tuple:
   * myInteger -> (myReal, myText)
   * 0 -> (0.0, zero)
   * 1 -> (1.0, one)
   * 2 -> (2.0, two)
   */
  
  @Test
  def equals {
    val ds = TestDataset.function_of_tuple
    //AsciiWriter.write(ds)
    val op = Selection("myText=one")
    val ds2 = op(ds)
    val data = ds2.toStringMap
    assertEquals(1, data("myText").length)
    assertEquals("1", data("myInteger").head)
  }
  
  @Test
  def regex_match {
    val ds = TestDataset.function_of_tuple
    val op = Selection("myText=~one")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myText").length)
    assertEquals("1", data("myInteger").head)
  }
  
  @Test
  def match_first_part {
    val ds = TestDataset.function_of_tuple
    val op = Selection("myText=~on.*")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myText").length)
    assertEquals("1", data("myInteger").head)
  }
  
  @Test
  def match_last_part {
    val ds = TestDataset.function_of_tuple
    val op = Selection("myText=~.*ne")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myText").length)
    assertEquals("1", data("myInteger").head)
  }
  
  @Test
  def match_middle {
    val ds = TestDataset.function_of_tuple
    val op = Selection("myText=~.*er.*")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myText").length)
    assertEquals("0", data("myInteger").head)
  }
  
  @Test
  def match_letter {
    val ds = TestDataset.function_of_tuple
    val op = Selection("myText=~.*e.*")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(2, data("myText").length)
    assertEquals("0", data("myInteger").head)
  }
  
  @Test
  def not_match {
    val ds = TestDataset.function_of_tuple
    val op = Selection("myText!=~.*e.*")
    val ds2 = op(ds)
    //AsciiWriter.write(ds2)
    val data = ds2.toStringMap
    assertEquals(1, data("myText").length)
    assertEquals("2", data("myInteger").head)
  }
  
}