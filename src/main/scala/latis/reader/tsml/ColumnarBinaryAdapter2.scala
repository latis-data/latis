package latis.reader.tsml

import scala.collection.immutable.{ Range => _ }
import scala.collection.mutable
import scala.collection.Searching._

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file._

import latis.data.Data
import latis.data.seq.DataSeq
import latis.dm._
import latis.ops.Operation
import latis.ops.filter._
import latis.reader.tsml.ml._
import latis.time._
import latis.util.StringUtils

import ColumnarBinaryAdapter2._

/**
 * Combines separate binary files representing individual Variables
 * into a single Dataset.
 */
class ColumnarBinaryAdapter2(tsml: Tsml) extends TsmlAdapter(tsml) {

  type Index = Array[Double]

  private lazy val order: ByteOrder =
    getProperty("byteOrder", "big-endian") match {
      case "big-endian" => ByteOrder.BIG_ENDIAN
      case "little-endian" => ByteOrder.LITTLE_ENDIAN
    }

  private lazy val domainVars: Seq[VariableMl] =
    getDomainVars(tsml.dataset)

  private val indexMap: mutable.Map[String, Index] =
    mutable.Map()

  private val ranges: mutable.LinkedHashMap[String, Range] = {
    val zero = mutable.LinkedHashMap[String, Range]()
    domainVars.foldLeft(zero)(_ += _.getName -> All)
  }

  private val operations: mutable.ArrayBuffer[Operation] =
    mutable.ArrayBuffer[Operation]()

  private val origLength: mutable.LinkedHashMap[String, Int] = {
    val zero = mutable.LinkedHashMap[String, Int]()
    domainVars.foldLeft(zero) { (m, v) =>
      val vname = v.getName
      m += vname -> (getFileLength(vname)/8)
    }
  }

  private def withChannel[A](name: String)(f: FileChannel => A): A = {
    val root = getUrlFile.toPath
    val path = root.resolve(s"${name}.bin")
    val chan = FileChannel.open(path, StandardOpenOption.READ)
    val res  = f(chan)
    chan.close()
    res
  }

  private def getFileLength(vname: String): Int =
    withChannel(vname)(_.size().toInt)

  override def handleOperation(op: Operation): Boolean =
    op match {
      case Selection(vname, o, v) if isDomainVar(vname) =>
        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
          val nt = convertTime(vname, v)
          new Selection(vname, o, nt)
        } else {
          op
        }
        operations += newOp
        true
      case NearestNeighborFilter(vname, v) if isDomainVar(vname) =>
        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
          val nt = convertTime(vname, v)
          new NearestNeighborFilter(vname, nt)
        } else {
          op
        }
        operations += newOp
        true
      case _: FirstFilter =>
        operations += op
        true
      case _ => false
    }

  private def isDomainVar(vname: String): Boolean =
    domainVars.exists(_.hasName(vname))

  private def convertTime(vname: String, value: String): String = {
    val domainVar = domainVars.find(_.hasName(vname)).get
    val units = domainVar.getMetadataAttributes.get("units").getOrElse {
      val msg = "Time variable must have units."
      throw new UnsupportedOperationException(msg)
    }
    val ts = TimeScale(units)
    Time.fromIso(value).convert(ts).getValue.toString
  }

  private def buildIndex(vname: String): Unit =
    indexMap += vname -> readData(vname)

  private def applyOperations: Unit =
    operations.foreach {
      case Selection(vname, op, value) =>
        indexMap.get(vname).foreach { index =>
          val range = queryIndex(index, op, value.toDouble)
          /*
           * This lookup should never fail (the only Selections
           * allowed are ones with names that come from the set of
           * domain variables used for these keys) but we can't
           * statically prove this.
           */
          ranges += vname -> range.intersect(ranges(vname))
        }
      case NearestNeighborFilter(vname, value) =>
        indexMap.get(vname).foreach { index =>
          val range = queryIndex(index, "~", value.toDouble)
          /*
           * This lookup should never fail (the only
           * NearestNeighborFilters allowed are ones with names that
           * come from the set of domain variables used for these
           * keys) but we can't statically prove this.
           */
          ranges += vname -> range.intersect(ranges(vname))
        }
      case _: FirstFilter =>
        val range = Bounds(0, 0)
        val vname = ranges.head._1
        ranges += vname -> ranges(vname).compose(range)
    }

  /*
   * We need to set the "length" of the inner function, which is
   * defined in the TSML, to the correct value based on the selections
   * we were given. This means that we need to have read the indices
   * and applied our operations before we can return the original
   * Dataset, because we cannot update metadata after that.
   */
  override def makeOrigDataset: Dataset = {
    val ds = super.makeOrigDataset

    // Build indices for domain variables that have some sort of
    // selection on them.
    operations.collect {
      case Selection(vname, _, _)          => vname
      case NearestNeighborFilter(vname, _) => vname
    }.distinct.foreach(buildIndex(_))

    applyOperations

    // Collect the length of each variable.
    val rs: Seq[Int] = ranges.toSeq.collect {
      case (_, r: Bounds) => r.length
      case (k, _)         => getFileLength(k)/8
    }

    // This is effectively treating the dataset as though it were
    // uncurried and limiting the number of samples in the uncurried
    // dataset.
    getProperty("limit").foreach { limit =>
      if (rs.product > limit.toInt) {
        throw new RuntimeException(
          s"Limit exceeded: requested ${rs.product}, allowed ${limit.toInt}"
        )
      }
    }

    replaceLengthMetadata(ds, rs)
  }

  // TODO: Copied from NetcdfAdapter3.
  private def replaceLengthMetadata(ds: Dataset, rs: Seq[Int]): Dataset = {
    def go(v: Variable, rs: Seq[Int]): Variable = v match {
      case f: Function =>
        val md = if (rs.head > 0) {
          f.getMetadata + ("length", s"${rs.head}")
        } else {
          f.getMetadata
        }

        Function(f.getDomain, go(f.getRange, rs.tail), md)
      case t @ Tuple(vs) => Tuple(vs.map(v => go(v, rs)), t.getMetadata)
      case _ => v
    }

    ds match {
      case Dataset(v) => Dataset(go(v, rs), ds.getMetadata)
    }
  }

  // Convenience method for reading data to build the index.
  private def readData(vname: String): Array[Double] =
    readData(vname, Seq(All))

  private def readData(vname: String, rs: Seq[Range]): Array[Double] =
    rs.length match {
      case 0 => Array[Double]()
      case 1 => rs(0) match {
        case Empty     => Array[Double]()
        case All       => readData1D(vname, Bounds(0, origLength(vname) - 1))
        case r: Bounds => readData1D(vname, r)
      }
      case 2 => (rs(0), rs(1)) match {
        case (Empty, _) | (_, Empty) => Array[Double]()
        case (All, r) =>
          val len = origLength.toSeq(0)._2 - 1
          val r2 = Bounds(0, len)
          readData(vname, Seq(r2, r))
        case (r, All) =>
          val len = origLength.toSeq(1)._2 - 1
          val r2 = Bounds(0, len)
          readData(vname, Seq(r, r2))
        case (r1: Bounds, r2: Bounds) =>
          readData2D(vname, Seq(r1, r2))
      }
      case _ =>
        val msg = "Can only read data up to 2D."
        throw new UnsupportedOperationException(msg)
    }

  def readData1D(vname: String, r: Bounds): Array[Double] =
    withChannel(vname) { channel =>
      val nSamples = r.length
      val arr: Array[Double] = Array.ofDim(nSamples)
      val bytes = ByteBuffer.allocate(nSamples * 8).order(order)

      // Skip to where we will start reading.
      channel.position(r.lower * 8)
      channel.read(bytes)

      bytes.rewind()
      bytes.asDoubleBuffer.get(arr)
      arr
    }

  private def readData2D(name: String, rs: Seq[Bounds]): Array[Double] =
    withChannel(name) { channel =>
      val outer = rs(0)
      val inner = rs(1)

      val nSamples = outer.length * inner.length
      val arr: Array[Double] = Array.ofDim(nSamples)

      // To read this we need to wrap a byte array.
      val bytes: Array[Byte] = Array.ofDim(nSamples * 8)

      for (i <- outer.lower to outer.upper) {
        val col = inner.lower
        val row = i
        val ind = {
          val origLen = origLength.toSeq(1)._2
          (row * origLen) + col
        }
        val len = inner.length * 8
        val off = (i - outer.lower) * len

        val bb = ByteBuffer.wrap(bytes, off, len).order(order)
        channel.position(ind * 8)
        channel.read(bb)
      }

      val bb = ByteBuffer.wrap(bytes).order(order)
      bb.asDoubleBuffer.get(arr)
      arr
    }

  private def readIntoCache(vname: String): Unit = {
    /*
     * Making the following assumption: If the variable we're reading
     * isn't a domain variable, we assume the variable is a function
     * of all the domain variables.
     */
    val rs: Seq[Range] = ranges.get(vname) match {
      case Some(r) => Seq(r)
      case None    => ranges.toSeq.map {
        case (_, v) => v
      }
    }
    val data = readData(vname, rs).map(Data(_))
    cache(vname, DataSeq(data))
  }

  override def init: Unit =
    getOrigScalars.foreach(s => readIntoCache(s.getName))

  override def close: Unit = ()
}

object ColumnarBinaryAdapter2 {
  // Assuming that the data are ordered in ascending order.
  //
  // TODO: Copied from NetcdfAdapter3 but uses a different range type.
  def queryIndex(index: Array[Double], op: String, v: Double): Range = {
    val len = index.length
    if (len > 0) {
      index.search(v) match {
        case Found(i) => op match {
          case ">"       =>
            if (i+1 < len) {
              Bounds(i+1, len-1)
            } else {
              Empty
            }
          case ">="      => Bounds(i, len-1)
          case "=" | "~" => Bounds(i, i)
          case "<="      => Bounds(0, i)
          case "<"       =>
            if (i-1 >= 0) {
              Bounds(0, i-1)
            } else {
              Empty
            }
        }
        case InsertionPoint(i) => op match {
          case ">" | ">=" =>
            if (i < len) {
              Bounds(i, len-1)
            } else {
              Empty
            }
          case "="        => Empty
          case "~"        =>
            if (i == 0) {
              // i = 0 implies our query is smaller than the smallest
              // value in the index
              Bounds(0, 0)
            } else if (i == len) {
              // i = len implies our query is larger than the largest
              // value in the index
              Bounds(len-1, len-1)
            } else {
              // Here we must determine the value in the index nearest
              // to the queried value.

              // We've already handled the i = 0 case, so i-1 should
              // be safe to access.
              val a = index(i-1)
              val b = index(i)
              // a < v < b

              // If v is equidistant from a and b (v - a = b - v), we
              // will round down. This is to be consistent with the
              // NearestNeighborInterpolation strategy.
              if (v - a <= b - v) {
                Bounds(i-1, i-1)
              } else {
                Bounds(i, i)
              }
            }
          case "<" | "<=" =>
            if (i > 0) {
              Bounds(0, i-1)
            } else {
              Empty
            }
        }
      }
    } else {
      Empty
    }
  }

  /**
   * Return a sequence of VariableMl corresponding to the domain
   * variables for this DatasetMl.
   */
  // TODO: This is shared with NetcdfAdapter3.
  def getDomainVars(ds: DatasetMl): Seq[VariableMl] = {
    def go(vml: VariableMl, acc: Seq[VariableMl]): Seq[VariableMl] = {
      vml match {
        case f: FunctionMl => go(f.range, acc :+ f.domain)  //TODO: consider tuple domain
        case t: TupleMl => t.variables.map(go(_,acc)).flatten
        case _ => acc
      }
    }

    go(ds.getVariableMl, Seq.empty)
  }

  sealed abstract class Range {
    def intersect(r2: Range): Range = (this, r2) match {
      case (Empty, _) => Empty
      case (_, Empty) => Empty
      case (r1, All)  => r1
      case (All, r2)  => r2
      case (Bounds(l1, u1), Bounds(l2, u2)) =>
        Bounds(Math.max(l1, l2), Math.min(u1, u2))
    }

    def compose(r2: Range): Range = (this, r2) match {
      case (Empty, _) => Empty
      case (_, Empty) => Empty
      case (r1, All)  => r1
      case (All, r2)  => r2
      case (Bounds(l1, u1), Bounds(l2, u2)) =>
        val vec = Vector.range(l1, u1 + 1).slice(l2, u2 + 1)
        if (vec.length > 0) {
          Bounds(vec.head, vec.last)
        } else {
          Empty
        }
    }
  }

  final case object All   extends Range
  final case object Empty extends Range
  final case class  Bounds(lower: Int, upper: Int) extends Range {
    def length: Int = (upper - lower) + 1
  }
}
