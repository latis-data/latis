package latis.dm

import org.junit._
import Assert._

class TestScalarComparison {

  @Test
  def compare_same_real {
    val r1 = Real(3.14)
    val c = r1 compare r1
    assertEquals(0, c)
  }
  
  @Test
  def compare_real_same_value {
    val r1 = Real(3.14)
    val r2 = Real(3.14)
    val c = r1 compare r2
    assertEquals(0, c)
  }
  
  @Test
  def compare_small_to_large_real {
    val r1 = Real(3.14)
    val r2 = Real(4.0)
    val c = r1 compare r2
    assertTrue(c < 0)
  }
  
  @Test
  def compare_large_to_small_real {
    val r1 = Real(3.14)
    val r2 = Real(3.0)
    val c = r1 compare r2
    assertTrue(c > 0)
  }
  
  @Test
  def compare_real_to_int {
    val i1 = Integer(4)
    val r2 = Real(3.0)
    val c = i1 compare r2
    assertTrue(c > 0)
  }
  
  @Test
  def compare_real_to_int_same_value {
    val i1 = Integer(3)
    val r2 = Real(3.0)
    val c = i1 compare r2
    assertEquals(0, c)
  }
  
  @Test
  def compare_same_text {
    val t = Text("A")
    val c = t compare t
    assertEquals(0, c)
  }
  
  @Test
  def compare_text_same_value {
    val t1 = Text("A")
    val t2 = Text("A")
    val c = t1 compare t2
    assertEquals(0, c)
  }
  
  @Test
  def compare_greater_text {
    val t1 = Text("B")
    val t2 = Text("A")
    val c = t1 compare t2
    assertTrue(c > 0)
  }
  
  @Test
  def compare_lesser_text {
    val t1 = Text("A")
    val t2 = Text("B")
    val c = t1 compare t2
    assertTrue(c < 0)
  }
  
  @Test
  def compare_text_with_trailing_space {
    val t1 = Text("A")
    val t2 = Text("A ")
    val c = t1 compare t2
    assertEquals(0, c)
  }
  
  @Test
  def compare_text_with_leading_space {
    val t1 = Text("A")
    val t2 = Text(" A")
    val c = t1 compare t2
    assertEquals(0, c)
  }
  
  //TODO: Time
  //TODO: diff units
}