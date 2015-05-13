package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.reader.tsml.LoopIterator
import latis.reader.tsml.RepeatIterator
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestColumnarBinaryAdapter extends AdapterTests{
  
  def datasetName = "binary_columns"
  
  @Test
  def rep_iterator {
    val it = new RepeatIterator(Iterator(1,2,3),2)
    assertEquals(List(1,1,2,2,3,3), it.toList)
  }
  
  @Test
  def loop_iterator {
    val it1 = new LoopIterator(Iterator(1,2))
    val it2 = Iterator(1,2,3,4)
    assertEquals(List((1,1),(2,2),(1,3),(2,4)), it1.zip(it2).toList)
  }
  
//  @Test
//  def test {
//    val ds = TsmlReader("datasets/test/nested_binary_columns.tsml").getDataset
//    AsciiWriter.write(ds)
//  }  
}