package latis.ops

import org.junit.Test
import latis.dm._
import org.junit.Assert._
import latis.metadata.Metadata

class TestFlatten {
  
  @Test
  def nested_index_functions = {
    val innerSamples = List(Sample(Index(), Real(Metadata("y"), 1.1)))
    val innerFunction = Function(innerSamples)
    val outerSamples = List(Sample(Index(), innerFunction))
    val ds = Dataset(Function(outerSamples))
    
    val ds2 = Flatten()(ds)
    //latis.writer.AsciiWriter.write(ds2)
    
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match { case Sample(_: Index, Real(d)) => assertEquals(1.1, d, 0.0) }
        assert(!it.hasNext)
      }
    }
  }
  
  @Test
  def nested_index_functions_multiple_samples = {
    //note depth first order
    val innerSamples1 = List(Sample(Index(), Real(Metadata("y"), 1.1)), Sample(Index(), Real(Metadata("y"), 2.2)))
    val innerSamples2 = List(Sample(Index(), Real(Metadata("y"), 3.3)), Sample(Index(), Real(Metadata("y"), 4.4)))
    val innerFunction1 = Function(innerSamples1)
    val innerFunction2 = Function(innerSamples2)
    val outerSamples = List(Sample(Index(), innerFunction1), Sample(Index(), innerFunction2))
    val ds = Dataset(Function(outerSamples))
    
    val ds2 = Flatten()(ds)
    //latis.writer.AsciiWriter.write(ds2)
    ds2 match {
      case Dataset(Function(it)) => {
        it.next match { case Sample(_: Index, Real(d)) => assertEquals(1.1, d, 0.0) }
        it.next match { case Sample(_: Index, Real(d)) => assertEquals(2.2, d, 0.0) }
        it.next match { case Sample(_: Index, Real(d)) => assertEquals(3.3, d, 0.0) }
        it.next match { case Sample(_: Index, Real(d)) => assertEquals(4.4, d, 0.0) }
        assert(!it.hasNext)
      }
    }
  }
  
}