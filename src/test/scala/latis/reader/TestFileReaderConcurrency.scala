package latis.reader

import latis.reader.tsml.ml._
import org.junit._
import Assert._
import latis.ops.Operation
import latis.writer.Writer
import java.io.FileOutputStream
import latis.reader.tsml.TsmlReader

class TestFileReaderConcurrency  {
  
  def make_runnable_request(ops: Seq[Operation], file: String): Runnable = new Runnable {
    def run {
      val writer = Writer(new FileOutputStream(file), "jsond")
      writer.write(TsmlReader("datasets/test/concurrency_test_iterative.tsml").getDataset(ops))
      println(System.nanoTime.toString + ": Wrote file " + file)
    }
  }
  
  //@Test
  def test {
    val ops = List[Operation]()
    new Thread(make_runnable_request(ops, "/data/tmp/test1")).start()
    new Thread(make_runnable_request(ops, "/data/tmp/test2")).start()
    new Thread(make_runnable_request(ops, "/data/tmp/test3")).start()

    println(System.nanoTime.toString + ": Started all threads.")
    //Give threads enough time to finish before junit whacks them.
    Thread.sleep(5000)
  }
  
}