package latis.ops

import org.junit._
import Assert._
import latis.dm._
import latis.metadata.Metadata

class TestRoundWithPrecision {
  val test: Metadata = Metadata("test")
  val testMetadata1: Metadata = Metadata(("name", "u"), ("precision", "2"))
  val testMetadata2: Metadata = Metadata(("name", "u"))

  val testInteger1: Integer = Integer(Metadata("t"), 1)
  val testInteger2: Integer = Integer(Metadata("t"), 2)
  val testInteger3: Integer = Integer(Metadata("t"), 3)
  val testInteger4: Integer = Integer(Metadata("t"), 4)

  @Test
  def test_round_from_metadata {
    val testSample1: Sample = Sample(testInteger1, Real(testMetadata1, 1.123))
    val testSample2: Sample = Sample(testInteger2, Real(testMetadata1, 2.123))
    val testSample3: Sample = Sample(testInteger3, Real(testMetadata1, 42.1))
    val testSample4: Sample = Sample(testInteger4, Real(testMetadata1, 123.42))

    val testSamples: List[Sample] = List(
                                          testSample1, 
                                          testSample2, 
                                          testSample3, 
                                          testSample4
                                        )

    val asserts: List[Double] = List(1.12, 2.12, 42.10, 123.42)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithPrecision("u")(ds)

    ds2 match {
      case Dataset(Function(it)) => {
        val data = it.toSeq
        for(i <- 0 until 4) {
          data(i) match {
            case Sample((_, Real(r))) => assertEquals(asserts(i), r, 0.0)
          }
        }
      }
    }
  }

  @Test
  def test_round_with_no_metadata {

    val testSample1: Sample = Sample(testInteger1, Real(testMetadata2, 1.123))
    val testSample2: Sample = Sample(testInteger2, Real(testMetadata2, 2.123))
    val testSample3: Sample = Sample(testInteger3, Real(testMetadata2, 42.1))
    val testSample4: Sample = Sample(testInteger4, Real(testMetadata2, 123.42))

    val testSamples: List[Sample] = List(
                                          testSample1, 
                                          testSample2, 
                                          testSample3, 
                                          testSample4
                                        )

    val asserts: List[Double] = List(1.123, 2.123, 42.1, 123.42)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithPrecision("u")(ds)

    ds2 match {
      case Dataset(Function(it)) => {
        val data = it.toSeq
        for(i <- 0 until 4) {
          data(i) match {
            case Sample((_, Real(r))) => assertEquals(asserts(i), r, 0.0)
          }
        }
      }
    }
  }

  @Test
  def test_user_specified_precision {
    val testSample: Sample = Sample(testInteger1, Real(testMetadata2, 2.123))

    val testSamples: List[Sample] = List(testSample)

    val asserts: List[Double] = List(2.12)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithPrecision("u", Some(2))(ds)

    ds2 match {
      case Dataset(Function(it)) => {
        val data = it.toSeq
        data(0) match {
          case Sample((_, Real(r))) => assertEquals(asserts(0), r, 0.0)
        }
      }
    }
  }

  @Test
  def test_user_overrides_metadata {
    val testMetadata: Metadata = Metadata(("name", "u"), ("precision", "5"))

    val testSample: Sample = Sample(testInteger1, Real(testMetadata, 2.1234567))

    val testSamples: List[Sample] = List(testSample)

    val asserts: List[Double] = List(2.123)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithPrecision("u", Some(3))(ds)

    ds2 match {
      case Dataset(Function(it)) => {
        val data = it.toSeq
        data(0) match {
          case Sample((_, Real(r))) => assertEquals(asserts(0), r, 0.0)
        }
      }
    }
  }

  @Test
  def test_invalid_user_precision {

    val testSample: Sample = Sample(testInteger1, Real(testMetadata2, 2.123))

    val testSamples: List[Sample] = List(testSample)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    try {
      val ds2: Dataset = RoundWithPrecision("u", Some(-1))(ds)
    }
    catch {
      case e: Error => assertEquals("Precision must be a postive integer", e.getMessage)
    }
  }
}
