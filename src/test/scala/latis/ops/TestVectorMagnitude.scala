package latis.ops

import org.junit.Assert.assertEquals
import org.junit.Test

import latis.dm.TestDataset
import latis.reader.tsml.TsmlReader

class TestVectorMagnitude {
  
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
  def test_paren {
    val ds = MathExpressionDerivation("A=(1+1)*3")(TestDataset.empty)
    assertEquals(6, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_sqrt {
    val ds = MathExpressionDerivation("A=sqrt(81)")(TestDataset.empty)
    assertEquals(9, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_sin {
    val ds = MathExpressionDerivation("A=sin(PI/2)")(TestDataset.empty)
    assertEquals(1, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_cos {
    val ds = MathExpressionDerivation("A=cos(PI/2)")(TestDataset.empty)
    assertEquals(0, ds.toSeq(0).getNumberData.doubleValue, 0.000000001)
  }
  
  @Test
  def test_acos {
    val ds = MathExpressionDerivation("A=acos(.5)")(TestDataset.empty)
    assertEquals(Math.PI/3, ds.toSeq(0).getNumberData.doubleValue, 0.000001)
  }
  
  @Test
  def test_atan2 {
    val ds = MathExpressionDerivation("A=atan2(0,5)")(TestDataset.empty)
    assertEquals(0, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_deg_to_radians {
    val ds = MathExpressionDerivation("A=deg_to_radians(180)")(TestDataset.empty)
    assertEquals(Math.PI, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_with_spaces {
    val ds = MathExpressionDerivation("A =(8* 3 ) +5% 2")(TestDataset.empty)
    assertEquals(25, ds.toSeq(0).getNumberData.doubleValue, 0.0)
  }
  
  @Test
  def test_tsml {
    val ds = TsmlReader("datasets/test/vecmag.tsml").getDataset
    val it = ds.findFunction.get.iterator
    it.next
    assertEquals(1.7320508075688772, it.next.findVariableByName("X").get.getNumberData.doubleValue, 0.0)
  }
  
}