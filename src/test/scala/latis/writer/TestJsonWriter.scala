package latis.writer

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.slf4j.Logging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.dm.TestDataset

class TestJsonWriter {
  
  def real = Writer("json").write(TestDataset.real)
  def integer = Writer("json").write(TestDataset.integer)
  def text = Writer("json").write(TestDataset.text)
  
  @Test
  def test = Writer("json").write(TestDataset.index_function)
}