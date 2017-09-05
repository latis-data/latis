package latis.dm

//import latis.dm.implicits._
import org.junit._
import Assert._
import latis.dm._
import latis.metadata._
import latis.time.Time
import latis.writer._


class TestDataset3 {

  //@Test
  def lazy_scalar = {
    case class S(b: Boolean)(q: => Int) {
      def f = if (b) println(q)
    }
    def x: Int = {println("x"); 3}
    val y: Int = {println("y"); 3}
    lazy val z: Int = {println("z"); 3}
    
    val s = S(true)(z)
    println("call 1")
    s.f
    println("call 2")
    s.f
  }
  
//  @Test
//  def scalar_with_value_from_function = {
//    def f = 3
//    val s = Integer3(f)
//    s match { case Integer3(v) => assertEquals(3l, v) }
//  }
  
//  val dataset = {
//    val a = Scalar3()(ScalarMetadata(Map("id" -> "A")))
//    val b = Scalar3()(ScalarMetadata(Map("id" -> "B")))
//    val x = Scalar3()(ScalarMetadata(Map("id" -> "X")))
//    val v = Function3(x, Tuple3(Seq(a,b))())
//    Dataset3("foo", v)
//  }
//  
//  @Test
//  def print = {
//    println(dataset)
//  }
//
//  @Test
//  def toSeq = {
//    dataset.toSeq.foreach(println)
//  }
//
//  @Test
//  def scalars = {
//    dataset.getScalars.foreach(println)
//  }
}