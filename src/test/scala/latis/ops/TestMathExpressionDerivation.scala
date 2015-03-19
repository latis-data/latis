package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.dm.TestDataset
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.dm.Dataset
import latis.dm.Function
import org.junit.Ignore

class TestMathExpressionDerivation {
  
  @Test
  def test_E {
    val ds = MathExpressionDerivation("A=E")(Dataset.empty)
    assertEquals(2.718281828459045, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_PI {
    val ds = MathExpressionDerivation("A=PI")(Dataset.empty)
    assertEquals(3.141592653589793, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_constant {
    val ds = MathExpressionDerivation("A=42")(Dataset.empty)
    assertEquals(42, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_add {
    val ds = MathExpressionDerivation("A=1+1")(Dataset.empty)
    assertEquals(2, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_subtract {
    val ds = MathExpressionDerivation("A=1-1")(Dataset.empty)
    assertEquals(0, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_times {
    val ds = MathExpressionDerivation("A=3*2")(Dataset.empty)
    assertEquals(6, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_divide {
    val ds = MathExpressionDerivation("A=4/2")(Dataset.empty)
    assertEquals(2, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_mod {
    val ds = MathExpressionDerivation("A=8%3")(Dataset.empty)
    assertEquals(2, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_pow {
    val ds = MathExpressionDerivation("A=3^3")(Dataset.empty)
    assertEquals(27, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_lt {
    val ds1 = MathExpressionDerivation("A=0<1")(Dataset.empty)
    val ds2 = MathExpressionDerivation("A=1<0")(Dataset.empty)
    assertEquals(1, ds1.toDoubles(0)(0), 0.0)
    assertEquals(0, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_and {
    val ds1 = MathExpressionDerivation("A=1&1")(Dataset.empty)
    val ds2 = MathExpressionDerivation("A=1&0")(Dataset.empty)
    assertEquals(1, ds1.toDoubles(0)(0), 0.0)
    assertEquals(0, ds2.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_paren {
    val ds = MathExpressionDerivation("A=(1+1)*3")(Dataset.empty)
    assertEquals(6, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_sqrt {
    val ds = MathExpressionDerivation("A=sqrt(81)")(Dataset.empty)
    assertEquals(9, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_sin {
    val ds = MathExpressionDerivation("A=sin(PI/2)")(Dataset.empty)
    assertEquals(1, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_cos {
    val ds = MathExpressionDerivation("A=cos(PI/2)")(Dataset.empty)
    assertEquals(0, ds.toDoubles(0)(0), 0.000000001)
  }
  
  @Test
  def test_acos {
    val ds = MathExpressionDerivation("A=acos(.5)")(Dataset.empty)
    assertEquals(Math.PI/3, ds.toDoubles(0)(0), 0.000001)
  }
  
  @Test
  def test_atan2 {
    val ds = MathExpressionDerivation("A=atan2(0,5)")(Dataset.empty)
    assertEquals(0, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_deg_to_radians {
    val ds = MathExpressionDerivation("A=deg_to_radians(180)")(Dataset.empty)
    assertEquals(Math.PI, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_with_spaces {
    val ds = MathExpressionDerivation("A =(8* 3 ) +5% 2")(Dataset.empty)
    assertEquals(25, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def test_tsml {
    val ds = TsmlReader("vecmag.tsml").getDataset
    //AsciiWriter.write(ds)
    val it = ds.unwrap.asInstanceOf[Function].iterator
    it.next
    assertEquals(1.7320508075688772, it.next.findVariableByName("X").get.getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def with_projection {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,a,b,c,X")
    val ds = TsmlReader("vecmag.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(1.7320508075688772, data("X")(1), 0.0)
    assertEquals(3.0, data("X")(2), 0.0)
  }
  
  @Test
  def dont_project_derived_param {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,a,b,c")
    val ds = TsmlReader("vecmag.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assert(!data.contains("X"))
  }
  
  @Test
  def dont_project_input_params {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,X")
    val ds = TsmlReader("vecmag.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(1.7320508075688772, data("X")(1), 0.0)
    assertEquals(3.0, data("X")(2), 0.0)
    assert(!data.contains("a"))
  }
  
  @Test
  def derived_field_as_input {
    val ds = TsmlReader("vecmag2.tsml").getDataset
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(2.7320508075688772, data("Y")(1), 0.0)
  }
  
  @Test @Ignore //cannot project derived Variable that depends on unprojected derived Variable
  def non_projected_derived_field_as_input {
    val ops = ArrayBuffer[Operation]()
    ops += Projection("t,Y")
    val ds = TsmlReader("vecmag2.tsml").getDataset(ops)
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(2.7320508075688772, data("Y")(1), 0.0)
  }
  
  //@Test
  def constant_in_tsml {
    val ds = TsmlReader("vecmag2.tsml").getDataset
    //AsciiWriter.write(ds)
    val data = ds.toDoubleMap
    assertEquals(123.4, data("Z").head, 0.0)
  }
  
  @Test
  def nested_operation {
    val ds = MathExpressionDerivation("A=sqrt(fabs(-81))")(Dataset.empty)
    //AsciiWriter.write(ds)
    assertEquals(9.0, ds.toDoubles(0)(0), 0.0)
  }
  
  @Test
  def mag {
    val ds = TsmlReader("vecmag2.tsml").getDataset
    val data = ds.toDoubleMap
    assertEquals(data("X")(2), data("X2")(2), 0.0001)
  }
}