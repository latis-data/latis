package latis.ops

import latis.dm._
import latis.metadata.Metadata
import org.junit.Assert._
import org.junit.Test

class TestApplyBitMask {

  private lazy val dataset: Dataset = {
    val smd = Metadata("name" -> "foo", "missing_value" -> "-999")
    val fmd = Metadata("name" -> "flag")
    val samples = List(
      Sample(Integer(0), Tuple(Real(smd, 1.0), Integer(fmd, 0))),
      Sample(Integer(1), Tuple(Real(smd, 2.0), Integer(fmd, 1))),
      Sample(Integer(2), Tuple(Real(smd, 3.0), Integer(fmd, 2))),
      Sample(Integer(3), Tuple(Real(smd, 4.0), Integer(fmd, 3))),
    )
    Dataset(Function(samples))
  }

  @Test
  def apply_bitmask(): Unit = {
    // Fill when flag has 2nd bit set
    val ds = new ApplyBitMask("foo", "flag & 2")(dataset)
    //latis.writer.Writer.fromSuffix("asc").write(ds)
    ds match {
      case Dataset(Function(it)) =>
        it.next match {
          case Sample(Integer(t), TupleMatch(Real(v), Integer(f), Real(fv))) =>
            assertEquals(0, t)
            assertEquals(1.0, v, 0.0)
            assertEquals(0, f)
            assertEquals(1.0, fv, 0.0)
        }
        it.next match {
          case Sample(Integer(t), TupleMatch(Real(v), Integer(f), Real(fv))) =>
            assertEquals(1, t)
            assertEquals(2.0, v, 0.0)
            assertEquals(1, f)
            assertEquals(2.0, fv, 0.0)
        }
        it.next match {
          case Sample(Integer(t), TupleMatch(Real(v), Integer(f), Real(fv))) =>
            assertEquals(2, t)
            assertEquals(3.0, v, 0.0)
            assertEquals(2, f)
            assertEquals(-999.0, fv, 0.0)
        }
        it.next match {
          case Sample(Integer(t), TupleMatch(Real(v), Integer(f), Real(fv))) =>
            assertEquals(3, t)
            assertEquals(4.0, v, 0.0)
            assertEquals(3, f)
            assertEquals(-999.0, fv, 0.0)
        }
    }
  }
}
