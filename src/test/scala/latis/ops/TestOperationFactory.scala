package latis.ops

import org.junit._
import Assert._

class TestOperationFactory {

  @Test def first: Unit = Operation("first")
  
  @Test def first_with_empty_arg: Unit = Operation("first", List(""))

  @Test def first_with_arg_error: Unit = {
    try {
      Operation("first", List("foo"))
      fail //shouldn't get here
    } catch {
      case e: UnsupportedOperationException => assertTrue(e.getMessage.endsWith("does not accept arguments"))
      case t: Throwable => t.printStackTrace; fail
    }
  }
  
  @Test def last: Unit = Operation("last")
  
  @Test def limit: Unit = Operation("limit", List("10"))
  
  @Test def limit_without_arg_error: Unit = {
    try {
      Operation("limit")
      fail //shouldn't get here
    } catch {
      case e: UnsupportedOperationException => assertTrue(e.getMessage.endsWith("requires arguments"))
      case _: Throwable => fail
    }
  }

  @Test def limit_with_two_args_error: Unit = {
    try {
      Operation("limit", List("1","2"))
      fail //shouldn't get here
    } catch {
      case e: UnsupportedOperationException => assertTrue(e.getMessage.endsWith("LimitFilter accepts only one argument"))
      case _: Throwable => fail
    }
  }

  @Test def limit_with_word_arg_error: Unit = {
    try {
      Operation("limit", List("foo"))
      fail //shouldn't get here
    } catch {
      case e: UnsupportedOperationException => assertTrue(e.getMessage.endsWith("requires an integer argument"))
      case _: Throwable => fail
    }
  }

  @Test def limit_with_double_arg_error: Unit = {
    try {
      Operation("limit", List("3.14"))
      fail //shouldn't get here
    } catch {
      case e: UnsupportedOperationException => assertTrue(e.getMessage.endsWith("requires an integer argument"))
      case _: Throwable => fail
    }
  }

}