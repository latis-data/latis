package latis.util

import org.junit._
import Assert._
import latis.dm.TestDataset

class TestDataMap {

  @Test
  def real {
    val map = DataMap.toDoubleMap(TestDataset.real)
    assertEquals(3.14, map("myReal")(0), 0.0)
  }
  
  @Test
  def scalar_has_only_one_value {
    val map = DataMap.toDoubleMap(TestDataset.real)
    assertEquals(1, map("myReal").length)
  }
  
  @Test
  def empty {
    val map = DataMap.toDoubleMap(TestDataset.empty)
    assertTrue(map.isEmpty)
  }
  
  @Test
  def scalars {
    val map = DataMap.toDoubleMap(TestDataset.scalars)
    assertEquals(3.14, map("myReal")(0), 0.0)
    assertEquals(42.0, map("myInteger")(0), 0.0)
    assertTrue(map("myText")(0).isNaN)
    assertEquals(1000.0, map("myRealTime")(0), 0.0)
  }
  
  @Test
  def function_of_scalar_with_data_from_kids {
    val map = DataMap.toDoubleMap(TestDataset.function_of_scalar_with_data_from_kids)
    //for ((n,v) <- map; d <- v) println(n + ": " + d)
    assertEquals(2.0, map("domain")(2), 0.0)
    assertEquals(0.0, map("range")(0), 0.0)
  }
  
  @Test
  def doubles_from_function_of_scalar_with_data_from_kids {
    val ds = DataMap.toDoubles(TestDataset.function_of_scalar_with_data_from_kids)
    //for (a <- ds; d <- a) println(d)
    assertEquals(2.0, ds(0)(2), 0.0)
    assertEquals(0.0, ds(1)(0), 0.0)
  }
  
  @Test
  def doubles_from_function_of_tuple_with_mixed_types {
    val ds = DataMap.toDoubles(TestDataset.function_of_tuple_with_mixed_types)
    //for (a <- ds; d <- a) println(d)
    assertEquals(2.0, ds(0)(2), 0.0)
    assertEquals(0.0, ds(1)(0), 0.0)
    assertTrue(ds(2)(0).isNaN)
  }
  
  @Test
  def strings_from_function_of_tuple_with_mixed_types {
    val ss = DataMap.toStrings(TestDataset.function_of_tuple_with_mixed_types)
    //for (a <- ss; s <- a) println(s)
    assertEquals("2", ss(0)(2))
    assertEquals("0.0", ss(1)(0))
    assertEquals("one", ss(2)(1))
  }
  
  //TODO:
  //unknown
  //bad name
  
}