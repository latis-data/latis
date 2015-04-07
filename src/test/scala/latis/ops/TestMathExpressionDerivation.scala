package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test
import latis.dm.TestDataset
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer

class TestMathExpressionDerivation {
  
  @Test
  def test_E {
    val ds = MathExpressionDerivation("A=E")(TestDataset.empty)
    assertEquals(2.718281828459045, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_PI {
    val ds = MathExpressionDerivation("A=PI")(TestDataset.empty)
    assertEquals(3.141592653589793, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_constant {
    val ds = MathExpressionDerivation("A=42")(TestDataset.empty)
    assertEquals(42, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_add {
    val ds = MathExpressionDerivation("A=1+1")(TestDataset.empty)
    assertEquals(2, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_subtract {
    val ds = MathExpressionDerivation("A=1-1")(TestDataset.empty)
    assertEquals(0, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_times {
    val ds = MathExpressionDerivation("A=3*2")(TestDataset.empty)
    assertEquals(6, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_divide {
    val ds = MathExpressionDerivation("A=4/2")(TestDataset.empty)
    assertEquals(2, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_mod {
    val ds = MathExpressionDerivation("A=8%3")(TestDataset.empty)
    assertEquals(2, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_pow {
    val ds = MathExpressionDerivation("A=3^3")(TestDataset.empty)
    assertEquals(27, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_lt {
    val ds1 = MathExpressionDerivation("A=0<1")(TestDataset.empty)
    val ds2 = MathExpressionDerivation("A=1<0")(TestDataset.empty)
    assertEquals(1, ds1.toSeq(0).getNumberData.doubleValue, 0.0)
    assertEquals(0, ds2.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_and {
    val ds1 = MathExpressionDerivation("A=1&1")(TestDataset.empty)
    val ds2 = MathExpressionDerivation("A=1&0")(TestDataset.empty)
    assertEquals(1, ds1.toSeq(0).getNumberData.doubleValue, 0.0)
    assertEquals(0, ds2.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_paren {
    val ds = MathExpressionDerivation("A=(1+1)*3")(TestDataset.empty)
    assertEquals(6, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_sqrt {
    val ds = MathExpressionDerivation("A=SQRT(81)")(TestDataset.empty)
    assertEquals(9, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_sin {
    val ds = MathExpressionDerivation("A=SIN(PI/2)")(TestDataset.empty)
    assertEquals(1, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_cos {
    val ds = MathExpressionDerivation("A=COS(PI/2)")(TestDataset.empty)
    assertEquals(0, ds.toSeq(0).getNumberData.doubleValue, 0.000000001)
  }
  
  @Test
  def test_acos {
    val ds = MathExpressionDerivation("A=ACOS(.5)")(TestDataset.empty)
    assertEquals(Math.PI/3, ds.toSeq(0).getNumberData.doubleValue, 0.000001)
  }
  
  @Test
  def test_atan2 {
    val ds = MathExpressionDerivation("A=ATAN2(0,5)")(TestDataset.empty)
    assertEquals(0, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_deg_to_radians {
    val ds = MathExpressionDerivation("A=DEG_TO_RAD(180)")(TestDataset.empty)
    assertEquals(Math.PI, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_with_spaces {
    val ds = MathExpressionDerivation("A =(8* 3 ) +5% 2")(TestDataset.empty)
    assertEquals(25, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_tsml {
    val ds = TsmlReader("vecmag.tsml").getDataset
    //AsciiWriter.write(ds)
    val it = ds.findFunction.get.iterator
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
  
  @Test
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
    val ds = MathExpressionDerivation("A=SQRT(FABS(-81))")(TestDataset.empty)
//    AsciiWriter.write(ds)
    assertEquals(9.0, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def mag {
    val ds = TsmlReader("vecmag2.tsml").getDataset
    val data = ds.toDoubleMap
    assertEquals(data("X")(2), data("X2")(2), 0.0001)
  }
  
  @Test 
  def magmag {
    val ds = MathExpressionDerivation("A=MAG(MAG(3,4)-2,4)")(TestDataset.empty)
    assertEquals(5.0, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
}