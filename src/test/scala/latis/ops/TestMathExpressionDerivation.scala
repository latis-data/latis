package latis.ops

import scala.collection.mutable.ArrayBuffer
import org.junit.Assert.assertEquals
import org.junit.Ignore
import org.junit.Test
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.TestDataset
import latis.metadata.Metadata
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter

class TestMathExpressionDerivation {
  
  private def trivialDataset: Dataset = {
    Dataset(
      Function(
        Seq(
          latis.dm.Integer(
            Metadata(Map("name" -> "one")),
            latis.data.Data(1)
          )
        )
      )
    )
  }
  
  @Test
  def test_missing_field {
    // If the passed dataset does not have all of the variables
    // required by the math expression, the derived field
    // should not be created. No exception should be thrown.
    val ds = MathExpressionDerivation("A=FOO*2")(trivialDataset)
    assertEquals(false, ds.toDoubleMap.contains("A"))
  }
  
  @Test
  def test_math_on_empty_dataset {
    val ds = MathExpressionDerivation("A=PI+E")(Dataset.empty)
    assert(ds.isEmpty)
  }
  
  @Test
  def test_plusplus {
    val ds = MathExpressionDerivation("A=1+2*3+4/5")(trivialDataset)
    assertEquals(7.8, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_divdivdiv {
    val ds = MathExpressionDerivation("A=1/2/3")(trivialDataset)
    assertEquals(0.16666666666, ds.toDoubleMap("A")(0), 0.0000001)
  }
  
  @Test
  def test_powpowpow {
    val ds = MathExpressionDerivation("A=2^3^4")(trivialDataset)
    assertEquals(Math.pow(2, Math.pow(3, 4)), ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_E {
    val ds = MathExpressionDerivation("A=E")(trivialDataset)
    assertEquals(2.718281828459045, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_PI {
    val ds = MathExpressionDerivation("A=PI")(trivialDataset)
    assertEquals(3.141592653589793, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_constant {
    val ds = MathExpressionDerivation("A=42")(trivialDataset)
    assertEquals(42, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_add {
    val ds = MathExpressionDerivation("A=1+1")(trivialDataset)
    assertEquals(2, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_subtract {
    val ds = MathExpressionDerivation("A=1-1")(trivialDataset)
    assertEquals(0, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_times {
    val ds = MathExpressionDerivation("A=3*2")(trivialDataset)
    assertEquals(6, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_divide {
    val ds = MathExpressionDerivation("A=4/2")(trivialDataset)
    assertEquals(2, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_mod {
    val ds = MathExpressionDerivation("A=8%3")(trivialDataset)
    assertEquals(2, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_pow {
    val ds = MathExpressionDerivation("A=3^3")(trivialDataset)
    assertEquals(27, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_lt {
    val ds1 = MathExpressionDerivation("A=0<1")(trivialDataset)
    val ds2 = MathExpressionDerivation("A=1<0")(trivialDataset)
    assertEquals(1, ds1.toDoubleMap("A")(0), 0.0)
    assertEquals(0, ds2.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_and {
    val ds1 = MathExpressionDerivation("A=1&1")(trivialDataset)
    val ds2 = MathExpressionDerivation("A=1&0")(trivialDataset)
    assertEquals(1, ds1.toDoubleMap("A")(0), 0.0)
    assertEquals(0, ds2.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_paren {
    val ds = MathExpressionDerivation("A=(1+1)*3")(trivialDataset)
    assertEquals(6, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_sqrt {
    val ds = MathExpressionDerivation("A=SQRT(81)")(trivialDataset)
    assertEquals(9, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_sin {
    val ds = MathExpressionDerivation("A=SIN(PI/2)")(trivialDataset)
    assertEquals(1, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_sin_and_plus {
    val ds = MathExpressionDerivation("A=SIN(PI/2)+10")(trivialDataset)
    assertEquals(11, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_cos {
    val ds = MathExpressionDerivation("A=COS(PI/2)")(trivialDataset)
    assertEquals(0, ds.toDoubleMap("A")(0), 0.000000001)
  }
  
  @Test
  def test_acos {
    val ds = MathExpressionDerivation("A=ACOS(.5)")(trivialDataset)
    assertEquals(Math.PI/3, ds.toDoubleMap("A")(0), 0.000001)
  }
  
  @Test
  def test_atan2 {
    val ds = MathExpressionDerivation("A=ATAN2(0,5)")(trivialDataset)
    assertEquals(0, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_deg_to_radians {
    val ds = MathExpressionDerivation("A=DEG_TO_RAD(180)")(trivialDataset)
    assertEquals(Math.PI, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_with_spaces {
    val ds = MathExpressionDerivation("A =(8* 3 ) +5% 2")(trivialDataset)
    assertEquals(25, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def test_tsml {
    val ds = TsmlReader("datasets/test/vecmag.tsml").getDataset
    //AsciiWriter.write(ds)
    val it = ds.unwrap.asInstanceOf[Function].iterator
    it.next
    assertEquals(1.7320508075688772, it.next.findVariableByName("X").get.getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def with_projection {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,a,b,c,X")
    val ds = TsmlReader("datasets/test/vecmag.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(1.7320508075688772, data("X")(1), 0.0)
    assertEquals(3.0, data("X")(2), 0.0)
  }
  
  @Test
  def dont_project_derived_param {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,a,b,c")
    val ds = TsmlReader("datasets/test/vecmag.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assert(!data.contains("X"))
  }
  
  @Test
  def dont_project_input_params {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,X")
    val ds = TsmlReader("datasets/test/vecmag.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(1.7320508075688772, data("X")(1), 0.0)
    assertEquals(3.0, data("X")(2), 0.0)
    assert(!data.contains("a"))
  }
  
  @Test
  def derived_field_as_input {
    val ds = TsmlReader("datasets/test/vecmag2.tsml").getDataset
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(2.7320508075688772, data("Y")(1), 0.0)
  }
  
  @Test @Ignore//cannot project derived Variable that depends on unprojected derived Variable
  def non_projected_derived_field_as_input {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,Y")
    val ds = TsmlReader("datasets/test/vecmag2.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assert(data.isEmpty)
  }
  
  @Test
  def constant_in_tsml {
    val ds = TsmlReader("datasets/test/vecmag2.tsml").getDataset
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(123.4, data("Z").head, 0.0)
  }
  
  @Test
  def nested_operation {
    val ds = MathExpressionDerivation("A=SQRT(FABS(-81))")(trivialDataset)
    //AsciiWriter.write(ds)
    assertEquals(9.0, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test
  def mag {
    val ds = TsmlReader("datasets/test/vecmag2.tsml").getDataset
    val data = ds.toDoubleMap
    assertEquals(data("X")(2), data("X2")(2), 0.0001)
  }
  
  @Test 
  def magmag {
    val ds = MathExpressionDerivation("A=MAG(MAG(3,4)-2,4)")(trivialDataset)
    assertEquals(5.0, ds.toDoubleMap("A")(0), 0.0)
  }
  
  @Test 
  def nested {
    val ds = MathExpressionDerivation("a = -z")(TestDataset.function_of_functions)
    assertEquals(-10, ds.toDoubleMap("a")(3), 0.0)
  }
  
  @Test 
  def nested_override {
    val ds = MathExpressionDerivation("y = z + 0.4")(TestDataset.function_of_functions)
    assertEquals(10, ds.toDoubleMap("y")(3), 0.0)
  }
  
  @Test
  def override_text_time {
    val ds = MathExpressionDerivation("myTime=myTime+8.64E7")(TestDataset.time_series)
    assertEquals(8.64E7, ds.toDoubleMap("myTime")(0), 0.0)
  }

  def assertEqualContents[T](s1: Seq[T], s2: Seq[T]) {
    if (s1.length != s2.length) {
      throw new AssertionError("Sequences did not have the same length")
    }
    
    var i: Int = -1
    for (i <- 0 until s1.length) {
      if (s1(i) != s2(i)) {
        throw new AssertionError(s"Sequences differed at index ${i}: expected ${s1(i)} but found ${s2(i)}")
      }
    }
  }
}