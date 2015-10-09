package latis.reader

import org.junit.Test
import org.junit.Assert._
import latis.reader.tsml.FormattedAsciiAdapter
import latis.reader.tsml.ml.Tsml

class TestFormattedAsciiAdapter extends AdapterTests{
  
  def datasetName = "formatted_ascii"
  
  @Test
  def int {
    val a = new FormattedAsciiAdapter(Tsml("formatted_ascii.tsml"))
    val r = a.formatToRegex("I3")
    assert("234".matches(r.toString))
    assert(" 34".matches(r.toString))
  }
  
  @Test
  def float {
    val a = new FormattedAsciiAdapter(Tsml("formatted_ascii.tsml"))
    val r = a.formatToRegex("F7.2")
    assert("2643.74".matches(r.toString))
    assert(!"123.234".matches(r.toString))
    assert("  -2.00".matches(r.toString))
  }
  
  @Test
  def string {
    val a = new FormattedAsciiAdapter(Tsml("formatted_ascii.tsml"))
    val r = a.formatToRegex("A5")
    assert("adfae".matches(r.toString))
    assert(!"adfe".matches(r.toString))
  }
  
  @Test
  def mix {
    val a = new FormattedAsciiAdapter(Tsml("formatted_ascii.tsml"))
    val r = a.formatToRegex("(2I2)/1A3,3F3.1")
    assert("32 4 abc0.03.1 .4".matches(r.toString))
  }

}