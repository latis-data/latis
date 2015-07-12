package latis.reader

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.util.iterator.LoopIterator
import latis.util.iterator.RepeatIterator
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Integer

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
  
  @Test
  def from_tsml {
    val ds = TsmlReader("datasets/test/nested_binary_columns.tsml").getDataset
    ds match {
      case Dataset(Function(f1it)) => f1it.toList.last.range match {
        case Function(f2it) => f2it.toList.last match {
          case Sample(Integer(d), Integer(r)) => {
            assertEquals(3, d)
            assertEquals(18, r)
          }
        }
      }
    }
  }  
}