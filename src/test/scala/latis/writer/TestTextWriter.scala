package latis.writer

import java.io.FileOutputStream
import org.junit._
import Assert._
import scala.io.Source
import latis.reader.tsml.TsmlReader
import java.io.DataOutputStream
import java.io.File
import latis.dm._
import latis.metadata.Metadata
import latis.reader.DatasetAccessor
import java.io.ByteArrayOutputStream

class TestTextWriter extends WriterTest{

  @Test
  def test_dap2 {
    test_writer(getDataset("dap2"),"txt")
  }
  @Test
  def test_fof {
    test_writer(getDataset(fof),"txt")
  }
  @Test
  def test_scalar {
    test_writer(getDataset("scalar"),"txt")
  }
  @Test
  def test_tsi {
    test_writer(getDataset("tsi"),"txt")
  }
  @Test
  def test_tof {
    test_writer(getDataset(tof),"txt")
  }
  
  //@Test
  def print_txt {
    print(fof, "txt")
  }
  
  //@Test 
  def write_txt_file {
    write_to_file(fof, "txt")
  }
  
  @Test
  def textWriter_with_precision_from_dataset = {
    // Basic testing for precision metadata
    val ds = DatasetAccessor.fromName("ascii_precision").getDataset
    val assertArray: Array[String] = Array(
                                            "0.12",
                                            "123.12"
                                          )
    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    val outputArray = output.toString().split("\n")
    val testArray = outputArray.map(v => v.split(", ")(1))
    assertEquals(assertArray(0), testArray(0))
    assertEquals(assertArray(1), testArray(1))
  }

  @Test
  def textWriter_with_sigfigs_from_dataset = {
    // Basic testing for sigfig metadata
    val ds = DatasetAccessor.fromName("ascii_sigfigs").getDataset
    val assertArray: Array[String] = Array(
                                            "0.12", "1.2e+02"
                                          )
    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    val outputArray = output.toString().split("\n")
    val testArray = outputArray.map(v => v.split(", ")(1))
    assertEquals(assertArray(0), testArray(0))
    assertEquals(assertArray(1), testArray(1))
  }

  @Test
  def real_prioritizes_precision = {
    val prec2_sigfig2: Metadata = Metadata(("precision", "2"), ("sigfigs", "2"))

    val testScalar: Real = Real(prec2_sigfig2, 1234.421)

    val ds = Dataset(Function(Seq(testScalar)))

    val cmpStr: String = "1234.42"

    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    // filter just gets rid of the newline, no need
    // to go with the same methods as above since
    // there's only one sample, so no need to separate
    // data lines, points, etc
    val out = output.toString().filter( _ >= ' ')
    assertEquals(cmpStr, out)
  }

  @Test
  def int_ignores_precision = {
    val prec0: Metadata = Metadata(("precision", "0"))
    val prec2: Metadata = Metadata(("precision", "2"))
    val prec2_sigfig2: Metadata = Metadata(("precision", "2"), ("sigfigs", "2"))

    val assertArray: Array[String] = Array("42", "42", "4.2e+03")

    val ds = Dataset(Function(Seq(
                                  Integer(prec0, 42),
                                  Integer(prec2, 42),
                                  Integer(prec2_sigfig2, 4212)
                                )))

    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    val testArray = output.toString().split("\n")

    for(i <- 0 until 2) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      assertEquals("Assertion failed for " + testSclr,
                    cmpStr, 
                    testSclr)
    }
    val cmpStr = assertArray(2)
    val testSclr = testArray(2)
    assertEquals(
                  "Assertion failed for ignoring precision when present with sigfigs",
                  cmpStr,
                  testSclr
                )
                  
  }

  @Test
  def varied_precision = {
    // Different precision values
    // Tests behaviour when precision exceeeds, equals, or 
    // is less than given data
    val prec0: Metadata = Metadata(("precision", "0"))
    val prec2: Metadata = Metadata(("precision", "2"))
    val prec5: Metadata = Metadata(("precision", "5"))
    val prec10: Metadata = Metadata(("precision", "10"))

    val assertArray: Array[String] = Array(
                                            "42",
                                            "42.42",
                                            "42.42345",
                                            "42.4234500000"
                                          )

    val ds = Dataset(Function(Seq(
                                  Real(prec0, 42.42345),                           
                                  Real(prec2, 42.42345),
                                  Real(prec5, 42.42345),
                                  Real(prec10, 42.42345)
                                )))

    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    val testArray = output.toString().split("\n")

    for(i <- 0 until 4) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      assertEquals("Assertion failed for " + testSclr,
                    cmpStr, 
                    testSclr)
    }
  }

  // Next two tests test similar behaviour to above
  // except with sigfigs
  
  @Test
  def varied_sigfigs_ints = {
    val sigf1: Metadata = Metadata(("sigfigs", "1"))
    val sigf2: Metadata = Metadata(("sigfigs", "2"))
    val sigf5: Metadata = Metadata(("sigfigs", "5"))
    val sigf10: Metadata = Metadata(("sigfigs", "10"))

    val assertArray: Array[String] =  Array(
                                             "4e+04",
                                             "4.2e+04",
                                             "42123",
                                             "42123.00000"
                                           )

    val ds = Dataset(Function(Seq(
                                  Integer(sigf1, 42123),
                                  Integer(sigf2, 42123),
                                  Integer(sigf5, 42123),
                                  Integer(sigf10, 42123)
                                )))
    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    val testArray = output.toString().split("\n")

    for(i <- 0 until 4) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      assertEquals("Assertion failed for " + testSclr,
                    cmpStr, 
                    testSclr)
    }
  }

  @Test
  def varied_sigfigs_reals = {
    val sigf1: Metadata = Metadata(("sigfigs", "1"))
    val sigf2: Metadata = Metadata(("sigfigs", "2"))
    val sigf5: Metadata = Metadata(("sigfigs", "5"))
    val sigf10: Metadata = Metadata(("sigfigs", "10"))

    val assertArray: Array[String] =  Array(
                                             "4",
                                             "4.2",
                                             "4.2123",
                                             "4.212300000"
                                           )

    val ds = Dataset(Function(Seq(
                                  Real(sigf1, 4.2123),
                                  Real(sigf2, 4.2123),
                                  Real(sigf5, 4.2123),
                                  Real(sigf10, 4.2123)
                                )))
    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    val testArray = output.toString().split("\n")

    for(i <- 0 until 4) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      assertEquals(
                    "Assertion failed for " + testSclr,
                    cmpStr, 
                    testSclr)
    }
  }
  
  @Test
  def fill_value = {
    //Note, empty space fill_value defined in latis.properties for txt writer.
    val ds = Dataset(Tuple(Real(1), Real(Double.NaN), Real(3)))
    //latis.writer.Writer.fromSuffix("txt").write(ds)
    val output = new ByteArrayOutputStream()
    Writer(output, "txt").write(ds)
    assertTrue(output.toString().startsWith("1.0, , 3.0"))
  }
}
