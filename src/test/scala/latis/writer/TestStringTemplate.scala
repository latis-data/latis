package latis.writer

import org.junit._
import Assert._

class TestStringTemplate extends WriterTest {
  
  @Test
  def test_apply {
    val tmpl = new StringTemplate("{{foo}} {{bar}}")
    val values = Map("foo" -> "foo", "bar" -> "bar")
    val transformed = tmpl.applyValues(values)
    assertEquals("foo bar", transformed)
  }
  
  @Test
  def test_missing_value_throws {
    val tmpl = new StringTemplate("{{foo}} {{bar}}")
    val values = Map("foo" -> "foo")
    try {
      val transformed = tmpl.applyValues(values)
      assert(false) // fail
    }
    catch {
      case e: Exception => assert(true) // pass
    } 
  }
  
  @Test
  def test_nested_templates {
    val tmpl = new StringTemplate("{{foo}} {{bar}}")
    val values = Map("foo" -> "{{bar}}", "bar" -> "{{foo}}")
    val transformed = tmpl.applyValues(values)
    assertEquals("{{bar}} {{foo}}", transformed)
  }
  
  @Test
  def test_hyphenated_names{
    val tmpl = new StringTemplate("{{foo-foo}} {{bar-bar}}")
    val values = Map("foo-foo" -> "foo", "bar-bar" -> "bar")
    val transformed = tmpl.applyValues(values)
    assertEquals("foo bar", transformed)
  }
  
  @Test
  def test_long_name {
    val longName = (0 until StringTemplate.MAX_NAME_SIZE).map(i => 'a').mkString
    val tmpl = new StringTemplate(s"{{$longName}} {{bar}}")
    val values = Map(longName -> "foo", "bar" -> "bar")
    val transformed = tmpl.applyValues(values)
    assertEquals("foo bar", transformed)
  }
}
