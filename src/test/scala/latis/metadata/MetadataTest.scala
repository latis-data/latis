package latis.metadata

import latis.dm._
import latis.dm.implicits._
import latis.writer._
import org.junit._
import Assert._

class MetadataTest {
  
  lazy val metadata = Metadata(Map("name" -> "value", "foo" -> "bar"))
  
  @Test
  def apply {
    val v = metadata("name")
    assertEquals("value", v)
  }
  
  @Test
  def apply_null {
    val v = metadata("baz")
    assertEquals(null, v)
  }
  
  @Test
  def apply_empty_null {
    val v = EmptyMetadata("baz")
    assertEquals(null, v)
  }
  
  @Test
  def get_some {
    val v = metadata.get("name")
    assertEquals(Some("value"), v)
  }
  
  @Test
  def get_none {
    val v = metadata.get("bar")
    assertEquals(None, v)
  }
  
  @Test
  def get_empty_none {
    val v = EmptyMetadata.get("bar")
    assertEquals(None, v)
  }
  
  @Test
  def name {
    val v = metadata.name
    assertEquals("value", v)
  }
  
  @Test
  def unknown_name {
    val v = EmptyMetadata.name
    assertEquals("unknown", v)
  }

  //---- Equality Tests -----------------------------------------------------//
  
  @Test
  def equals_self {
    val md1 = Metadata("value")
    val md2 = md1
    assertEquals(md1, md2)
  }
  
  @Test
  def equals_empty {
    val md1 = Metadata.empty
    val md2 = EmptyMetadata
    assertEquals(md1, md2)
  }
  
  @Test
  def equals_diff_constructor {
    val md1 = Metadata("value")
    val md2 = Metadata(Map("name" -> "value"))
    assertEquals(md1, md2)
  }
  
  @Test
  def equals_multiple_properties_diff_order {
    //TODO: is this sufficient?
    val md1 = Metadata(Map("name" -> "value", "foo" -> "bar"))
    val md2 = Metadata(Map("foo" -> "bar", "name" -> "value"))
    assertEquals(md1, md2)
  }
  
  
  //-------------------------------------------------------------------------//
}