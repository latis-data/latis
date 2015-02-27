package latis.server

import org.junit._
import Assert._
import latis.ops.filter.Selection
import latis.ops.Projection
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter

class TestDapConstraintParser {

  @Test
  def no_constraints {
    val args = "".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(0, ops.length)
  }

  @Test
  def selection {
    val args = "&time>0".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(1, ops.length)
    assertTrue(ops.head.isInstanceOf[Selection])
  }

  @Test
  def two_selections {
    val args = "&time>0&time<10".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(2, ops.length)
    assertTrue(ops(0).isInstanceOf[Selection])
    assertTrue(ops(1).isInstanceOf[Selection])
  }

  @Test
  def projection {
    val args = "time".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(1, ops.length)
    assertTrue(ops.head.isInstanceOf[Projection])
  }

  @Test
  def two_projections {
    val args = "time,value".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(1, ops.length) //still treated as one operation
    assertTrue(ops.head.isInstanceOf[Projection])
  }
  
  @Test
  def filter {
    val args = "&first()".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(1, ops.length) 
    assertTrue(ops.head.isInstanceOf[FirstFilter])
  }
  
  @Test
  def two_filters {
    val args = "&first()&last()".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(2, ops.length) 
    assertTrue(ops(0).isInstanceOf[FirstFilter])
    assertTrue(ops(1).isInstanceOf[LastFilter])
  }
  
  @Test
  def projection_selection_filter {
    val args = "time&time<10&last()".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
    assertEquals(3, ops.length) 
    assertTrue(ops(0).isInstanceOf[Selection])
    assertTrue(ops(1).isInstanceOf[LastFilter])
    assertTrue(ops(2).isInstanceOf[Projection]) //Note, projection ends up last
  }
  
  @Test(expected = classOf[UnsupportedOperationException])
  def bad_filter {
    val args = "time&time<10&last".split("&")
    val ops = (new DapConstraintParser).parseArgs(args)
  }
}