package latis.dm

import org.junit._
import Assert._

class TestNaught {

  @Test
  def test_empty_seq_returns_empty_fn_and_naughts {
    val fn = Function(Seq())
    
    assert(fn.iterator.isEmpty)
    
    fn.getDomain match {
      case _: Naught => /* pass: do nothing */
      case _ => fail()
    }
    
    fn.getRange match {
      case _: Naught => /* pass: do nothing */
      case _ => fail()
    }
  }
  
  @Test
  def empty_fn_equals_empty_fn {
    assertEquals(Function.empty, Function(Seq()))
  }
  
  @Test
  def test_empty_fn_dataset_is_not_empty {
    val ds = Dataset(Function.empty)
    assert(!ds.isEmpty)
  }
  
  @Test
  def test_find_variable_by_name_returns_none {
    val ds = Dataset(Function.empty)
    
    ds.findVariableByName("Flurble") match {
      case None => /* pass: do nothing */
      case Some(_) => fail()
    }
    ds.findVariableByName("") match {
      case None => /* pass: do nothing */
      case Some(_) => fail()
    }
  }
}