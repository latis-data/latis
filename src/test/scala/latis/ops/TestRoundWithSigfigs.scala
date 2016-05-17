package latis.ops

import org.junit._
import Assert._
import latis.dm._
import latis.metadata.Metadata

class TestRoundWithSigfigs {
  val test: Metadata = Metadata("test")
  val testMetadata1: Metadata = Metadata(("name", "u"), ("sigfigs", "2"))
  val testMetadata2: Metadata = Metadata(("name", "u"))

  val testInteger1: Integer = Integer(Metadata("t"), 1)
  val testInteger2: Integer = Integer(Metadata("t"), 2)
  val testInteger3: Integer = Integer(Metadata("t"), 3)
  val testInteger4: Integer = Integer(Metadata("t"), 4)

  @Test
  def test_round_from_metadata_reals {
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

    val asserts: List[Double] = List(1.1, 2.1, 42, 120.0) 

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithSigfigs("u")(ds)

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
  def test_round_from_metadata_ints {
    val testSample1: Sample = Sample(testInteger1, Integer(testMetadata1, 123))
    val testSample2: Sample = Sample(testInteger2, Integer(testMetadata1, 23))
    val testSample3: Sample = Sample(testInteger3, Integer(testMetadata1, 42424))
    val testSample4: Sample = Sample(testInteger4, Integer(testMetadata1, 1))

    val testSamples: List[Sample] = List(
                                          testSample1, 
                                          testSample2, 
                                          testSample3, 
                                          testSample4
                                        )

    val asserts: List[Double] = List(120, 23, 42000, 1) 

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithSigfigs("u")(ds)

    ds2 match {
      case Dataset(Function(it)) => {
        val data = it.toSeq
        for(i <- 0 until 4) {
          data(i) match {
            case Sample((_, Integer(l))) => assertEquals(asserts(i), l, 0.0)
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
    val ds2: Dataset = RoundWithSigfigs("u")(ds)

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
  def test_user_specified_sigfigs_reals {
    val testSample: Sample = Sample(testInteger1, Real(testMetadata2, 2.123))

    val testSamples: List[Sample] = List(testSample)

    val asserts: List[Double] = List(2.1)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithSigfigs("u", Some(2))(ds)

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
  def test_user_specified_sigfigs_ints {
    val testSample: Sample = Sample(testInteger1, Integer(testMetadata2, 2123))

    val testSamples: List[Sample] = List(testSample)

    val asserts: List[Double] = List(2120)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithSigfigs("u", Some(3))(ds)

    ds2 match {
      case Dataset(Function(it)) => {
        val data = it.toSeq
        data(0) match {
          case Sample((_, Integer(l))) => assertEquals(asserts(0), l, 0.0)
        }
      }
    }
  }

  @Test
  def test_user_overrides_metadata {
    val testMetadata: Metadata = Metadata(("name", "u"), ("sigfigs", "5"))

    val testSample: Sample = Sample(testInteger1, Real(testMetadata, 2.1234567))

    val testSamples: List[Sample] = List(testSample)

    val asserts: List[Double] = List(2.12)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    val ds2: Dataset = RoundWithSigfigs("u", Some(3))(ds)

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
  def test_invalid_user_sigfigs {

    val testSample: Sample = Sample(testInteger1, Real(testMetadata2, 2.123))

    val testSamples: List[Sample] = List(testSample)

    val ds: Dataset = Dataset(Function(testSamples, test), test)
    try {
      val ds2: Dataset = RoundWithSigfigs("u", Some(-1))(ds)
    }
    catch {
      case e: Error => assertEquals("Sigfigs must be greater than zero", e.getMessage)
    }
  }
}
