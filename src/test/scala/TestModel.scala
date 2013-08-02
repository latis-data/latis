import latis.dm._
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import java.nio.ByteBuffer
import latis.data.buffer.ByteBufferData

class TestModel  {
  
  @Test
  def test_array_data {
    val data = Data(Vector(1.0,2.0,3.0))
    for (d <- data.iterator) println(d)
  }
  

  @Test
  def real_addition {
    val v = Real("pi", 3.14)
    new CsvWriter(System.out).write(v)
    new CsvWriter(System.out).write(v + v)
    //println(v)
    //println(v + v)
  }
  
  @Test
  def test_tuple_from_reals {
    val pi = Real(3.14)
    val e = Real(2.71828)
    
    val tup = Tuple(pi,e)
    
    new CsvWriter(System.out).write(tup)
    //println(tup)
  }
  
  //TODO: test using Tuple's data
  
  @Test
  def test_function_with_data_from_real_kids {
    val domain = List(0.,1.,2.,3.,4.)
    val range = List(1.,2.,3.,4.,5.)
    val f = Function(domain, range)
    val f2 = f + Real(1)
    println(f2)
    //for ((d,r) <- f.iterator) println(d.toString + " -> " + r)
    //for ((d,r) <- f2(0)(0).asInstanceOf[Function].iterator) println(d.toString + " -> " + r) 
    new AsciiWriter(System.out).write(f2)
    //new CsvWriter(System.out).write(f2)
    //new JsonWriter(System.out).write(f2)
  }
  
  @Test
  def test_function_with_data_from_tuple_range {
    val data = List(List(0.,1.,2.,3.,4.), List(1.,2.,3.,4.,5.), List(11.,12.,13.,14.,15.))
    val f = Function(data)
    val f2 = f + Real(1)
    new AsciiWriter(System.out).write(f2)
  }
  
  //def test_function_with_data_from_tuple_domain
  
  @Test
  def test_function_with_byte_buffer_data {
    val length = 5 //samples
    val nvar = 3 //number of variables
    val n = length * nvar
    val bb = (0 until n).foldLeft(ByteBuffer.allocate(n * 8))(_.putDouble(_)).flip.asInstanceOf[ByteBuffer]
    val data = new ByteBufferData(bb, 8 * nvar)
    val domain = Real("time")
    val range = Tuple(Real("a"), Real("b"))
    val f = Function(domain, range, data)
    
    new AsciiWriter(System.out).write(f)
  }
  
}