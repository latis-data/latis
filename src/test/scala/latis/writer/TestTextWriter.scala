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
  def makeScalar_with_precision_from_dataset = {
    // Basic testing for precision metadata
    val ds = DatasetAccessor.fromName("ascii_precision").getDataset
    val assertArray: Array[String] = Array(
                                            "0.12",
                                            "123.12"
                                          )
    ds match {
      case Dataset(Function(it)) => for(cmpStr <- assertArray) it.next match {
        case Sample(_, s:Scalar) => {
          assertEquals(cmpStr, latis.writer.TextWriter.makeScalar(s))
        }
      }
    }
  }

  @Test
  def makeScalar_with_sigfigs_from_dataset = {
    // Basic testing for sigfig metadata
    val ds = DatasetAccessor.fromName("ascii_sigfigs").getDataset
    val assertArray: Array[String] = Array(
                                            "0.12",
                                            "1.2e+02"
                                          )
    ds match {
      case Dataset(Function(it)) => for(cmpStr <- assertArray) it.next match {
        case Sample(_, s:Scalar) => {
          assertEquals(cmpStr, latis.writer.TextWriter.makeScalar(s))
        }
      }
    }
  }

  @Test
  def real_prioritizes_precision = {
    val prec2_sigfig2: Metadata = Metadata(("precision", "2"), ("sigfigs", "2"))

    val testScalar: Scalar = Scalar(prec2_sigfig2, 1234.421)

    val cmpStr: String = "1234.42"

    assertEquals(cmpStr, latis.writer.TextWriter.makeScalar(testScalar))
  }

  @Test
  def int_ignores_precision = {
    val prec0: Metadata = Metadata(("precision", "0"))
    val prec2: Metadata = Metadata(("precision", "2"))
    val prec2_sigfig2: Metadata = Metadata(("precision", "2"), ("sigfigs", "2"))

    val assertArray: Array[String] = Array("42", "42", "4.2e+03")

    val testArray: Array[Scalar] = Array(
                                          Scalar(prec0, 42),
                                          Scalar(prec2, 42),
                                          Scalar(prec2_sigfig2, 4212)
                                        )

    for(i <- 0 until 2) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      assertEquals("Assertion failed for " +
                    testSclr.getValue +
                    " with precision " +
                    testSclr.getMetadata("precision"),
                    cmpStr, 
                    latis.writer.TextWriter.makeScalar(testSclr))
    }
    val cmpStr = assertArray(2)
    val testSclr = testArray(2)
    assertEquals(
                  "Assertion failed for ignoring precision when present with sigfigs",
                  cmpStr,
                  latis.writer.TextWriter.makeScalar(testSclr)
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
    val testArray: Array[Scalar] = Array(
                                          Scalar(prec0, 42.42345),
                                          Scalar(prec2, 42.42345),
                                          Scalar(prec5, 42.42345),
                                          Scalar(prec10, 42.42345)
                                        )
    for(i <- 0 until 4) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      assertEquals("Assertion failed for " +
                    testSclr.getValue +
                    " with precision " +
                    testSclr.getMetadata("precision"),
                    cmpStr, 
                    latis.writer.TextWriter.makeScalar(testSclr))
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

    val testArray: Array[Scalar] = Array(
                                          Scalar(sigf1, 42123),
                                          Scalar(sigf2, 42123),
                                          Scalar(sigf5, 42123),
                                          Scalar(sigf10, 42123)
                                        )

    for(i <- 0 until 4) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      println(testSclr.getValue)
      assertEquals(
                    "Assertion failed for " +
                    testSclr.getValue +
                    " with significant figures " +
                    testSclr.getMetadata("sigfigs"),
                    cmpStr, 
                    latis.writer.TextWriter.makeScalar(testSclr)
                  )
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

    val testArray: Array[Scalar] = Array(
                                          Scalar(sigf1, 4.2123),
                                          Scalar(sigf2, 4.2123),
                                          Scalar(sigf5, 4.2123),
                                          Scalar(sigf10, 4.2123)
                                        )

    for(i <- 0 until 4) {
      val cmpStr = assertArray(i)
      val testSclr = testArray(i)
      println(testSclr.getValue)
      assertEquals(
                    "Assertion failed for " +
                    testSclr.getValue +
                    " with significant figures " +
                    testSclr.getMetadata("sigfigs"),
                    cmpStr, 
                    latis.writer.TextWriter.makeScalar(testSclr)
                  )
    }
  }
}
