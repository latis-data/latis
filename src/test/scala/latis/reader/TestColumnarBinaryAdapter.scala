package latis.reader

import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer

import scala.Iterator
import scala.collection.mutable.ArrayBuffer

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Sample
import latis.reader.tsml.TsmlReader
import latis.util.FileUtils
import latis.util.iterator.LoopIterator
import latis.util.iterator.RepeatIterator
import latis.writer.AsciiWriter

class TestColumnarBinaryAdapter extends AdapterTests{
  
  def datasetName = "binary_columns"
  
  var files = ArrayBuffer[File]()
  
  @Before
  def make_files {
    val dir = FileUtils.getTmpDir
    val myTime = Seq("1970/01/01","1970/01/02","1970/01/03")
    val myInt = Seq(1,2,3)
    val myReal = Seq(1.1,2.2,3.3)
    val myText = Seq("A   ","B   ","C   ")
    val timeBytes = ByteBuffer.allocate(60)
    myTime.foreach(_.foreach(timeBytes.putChar(_)))
    val intBytes = ByteBuffer.allocate(24)
    myInt.foreach(intBytes.putLong(_))
    val realBytes = ByteBuffer.allocate(24)
    myReal.foreach(realBytes.putDouble(_))
    val textBytes = ByteBuffer.allocate(24)
    myText.foreach(_.foreach(textBytes.putChar(_)))
    val timeFile = new File(dir, "myTime.bin")
    val intFile = new File(dir, "myInt.bin")
    val realFile = new File(dir, "myReal.bin")
    val textFile = new File(dir, "myText.bin")
    files += timeFile += intFile += realFile += textFile
    val timeIS = new FileOutputStream(timeFile)
    val intIS = new FileOutputStream(intFile)
    val realIS = new FileOutputStream(realFile)
    val textIS = new FileOutputStream(textFile)
    timeIS.write(timeBytes.array)
    intIS.write(intBytes.array)
    realIS.write(realBytes.array)
    textIS.write(textBytes.array)
    timeIS.close
    intIS.close
    realIS.close
    textIS.close
  }
  
  @After
  def close_files {
    files.foreach(_.delete)
  }
  
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
  
  @Test @Ignore //broke by adding scalar to range: a -> (myInt, b2 -> c)
  def from_tsml {
    val ds = TsmlReader("datasets/test/nested_binary_columns.tsml").getDataset
    AsciiWriter.write(ds)
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