package latis.ops

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.metadata.Metadata
import latis.data.SampledData

class TestReplaceMissing {

  @Test
  def replace_with_number = {
    val ds1 = TestDataset.function_of_scalar_with_nan
    val ds2 = ReplaceMissingOperation(-999)(ds1)
    ds2 match {
      case Dataset(Function(it)) => it.next; it.next match {
        case Sample(_, Real(d)) => assertEquals(-999.0, d, 0.0)
      }
    }
  }
  
  @Test
  def replace_with_number_as_string = {
    val ds1 = TestDataset.function_of_scalar_with_nan
    val ds2 = ReplaceMissingOperation(List("-999"))(ds1)
    ds2 match {
      case Dataset(Function(it)) => it.next; it.next match {
        case Sample(_, Real(d)) => assertEquals(-999.0, d, 0.0)
      }
    }
  }
  
  @Test
  def replace_missing_value_from_metadata = {
    val domain = Real(Metadata("name" -> "foo"))
    val range = Real(Metadata("missing_value" -> "0"))
    val data = SampledData.fromValues(List(0,1,2), List(3,0,5))
    val ds = Dataset(Function(domain, range, data = data))
    val ds2 = ReplaceMissingOperation(-999)(ds)
    ds2 match {
      case Dataset(Function(it)) => it.next; it.next match {
        case Sample(_, Real(d)) => assertEquals(-999.0, d, 0.0)
      }
    }
  }
  
  @Test
  def replace_integer_missing_value_from_metadata = {
    val domain = Real(Metadata("name" -> "foo"))
    val range = Integer(Metadata("missing_value" -> "0"))
    val data = SampledData.fromValues(List(0,1,2), List(3,0,5))
    val ds = Dataset(Function(domain, range, data = data))
    val ds2 = ReplaceMissingOperation(-999)(ds)
    ds2 match {
      case Dataset(Function(it)) => it.next; it.next match {
        case Sample(_, Integer(d)) => assertEquals(-999, d)
      }
    }
  }

}




