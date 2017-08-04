package latis.reader

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file._

import org.junit._
import org.junit.Assert._

import latis.dm._
import latis.ops.filter.Selection
import latis.ops.filter.FirstFilter
import latis.util.FileUtils

// Can't subclass AdapterTests here because this adapter does not
// support all the types typically used.
class TestColumnarBinaryAdapter2 {

  // This must match the length defined in the TSML file.
  val len = 5

  val time = Array.range(0, len).map(_.toDouble)
  val wavelength = Array.range(0, len).map(_.toDouble)
  val irradiance = Array.range(0, len * len).map(_.toDouble)

  val dir = FileUtils.getTmpDir.toPath
  val files: Seq[(String, Array[Double])] = Seq(
    ("time.bin", time),
    ("wavelength.bin", wavelength),
    ("irradiance.bin", irradiance)
  )

  @Before
  def makeFiles: Unit = files.foreach {
    case (fileName: String, data: Array[Double]) =>
      val bytes = ByteBuffer.allocate(data.length * 8)
      data.foreach(bytes.putDouble(_))
      bytes.rewind()

      val chan = {
        val path = dir.resolve(fileName)
        FileChannel.open(
          path,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE
        )
      }
      chan.write(bytes)
      chan.close()
  }

  @After
  def cleanup: Unit = files.foreach {
    case (fileName, _) => dir.resolve(fileName).toFile.delete()
  }

  @Test
  def timeSlice: Unit = {
    val ops = Seq(
      Selection("time >= 1991-09-15"),
      Selection("time < 1991-09-16")
    )

    val ds = DatasetAccessor.fromName("binary_columns_2").getDataset(ops)
    ds match {
      case Dataset(Function(it)) =>
        it.next match {
          case Sample(Real(t), Function(it)) =>
            // Because this is the first sample and the units are days
            // since the date of the first sample, we expect the time
            // to be 0.
            assertEquals(0.0, t, 0.0)

            val samples = it.toSeq

            // Because this query should give all wavelengths, we
            // expect there to be as many samples as there are
            // wavelengths.
            assertEquals(len, samples.length)

            wavelength.zip(samples).foreach {
              case (w1, Sample(Real(w2), _)) => assertEquals(w1, w2, 0.0)
            }

            // Because this is the first time sample we know the
            // irradiance is just the first 'len' values.
            irradiance.take(len).zip(samples).foreach {
              case (f1, Sample(Real(f2), _)) => assertEquals(f1, f2, 0.0)
            }
        }

        // There should only be one time sample.
        assertFalse(it.hasNext)
    }
  }

  @Test
  def wavelengthSlice: Unit = {
    val w1 = 2
    val ops = Seq(Selection(s"wavelength ~ $w1"))

    val ds = DatasetAccessor.fromName("binary_columns_2").getDataset(ops)
    ds match {
      case Dataset(Function(it)) =>
        val samples = it.toSeq

        // There should be one sample for each time sample.
        assertEquals(len, samples.length)
        time.zip(samples).foreach {
          case (t1, Sample(Real(t2), _)) => assertEquals(t1, t2, 0.0)
        }

        // Because of how we've defined irradiance (natural numbers up
        // to len * len) the set of values selected will start with
        // the value of the wavelength selected and increase by len.
        val irradiance = Range(w1, len * len, len).toSeq.map(_.toDouble)

        irradiance.zip(samples).foreach {
          case (f1, Sample(_, Function(it))) =>
            it.next match {
              case Sample(Real(w2), Real(f2)) =>
                assertEquals(f1, f2, 0.0)

                // Wavelength should be equal to the one we selected.
                assertEquals(w1, w2, 0.0)
            }

            // There should be a single wavelength.
            assertFalse(it.hasNext)
        }
    }
  }

  @Test
  def firstFilter: Unit = {
    val ops = Seq(FirstFilter())

    val ds = DatasetAccessor.fromName("binary_columns_2").getDataset(ops)
    ds match {
      case Dataset(Function(it)) =>
        it.next match {
          case Sample(Real(t), _) => assertEquals(0.0, t, 0.0)
        }

        assertFalse(it.hasNext)
    }
  }
}
