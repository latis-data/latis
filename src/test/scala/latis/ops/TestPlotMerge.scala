package latis.ops

import org.junit.Test
import org.junit.Ignore
import org.junit.Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.data.Data
import latis.writer.AsciiWriter
import latis.metadata.Metadata

class TestPlotMerge {

  @Test
  def TestNoResampleMerge {
    val intVar = Integer(Metadata("dataVal"), 0)
    val xVar = Real(Metadata("lon"), 0.0)
    val yVar = Real(Metadata("lat"), 0.0)
    val domain1: Array[Tuple] = Array(
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.1))),
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.2))),
                                  Tuple(xVar(Data(0.2)), yVar(Data(0.1))),
                                  Tuple(xVar(Data(0.2)), yVar(Data(0.2)))
                                )

    val domain2: Array[Tuple] = Array(
                                  Tuple(xVar(Data(0.05)), yVar(Data(0.05))),
                                  Tuple(xVar(Data(0.05)), yVar(Data(0.1))),
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.05))),
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.1)))
                                )

    val range1: Array[Variable] = Array(
                                    intVar(Data(1)),
                                    intVar(Data(2)),
                                    intVar(Data(3)),
                                    intVar(Data(4))
                                  )

    val range2: Array[Variable] = Array(
                                    intVar(Data(5)),
                                    intVar(Data(6)),
                                    intVar(Data(7)),
                                    intVar(Data(8))
                                  )

    val correct = Array(
                    Array(0.1, 0.1, 1, "8"),
                    Array(0.1, 0.2, 2, "None"),
                    Array(0.2, 0.1, 3, "None"),
                    Array(0.2, 0.2, 4, "None")
                  )

    val ds1 = Dataset(Function(domain1.zip(range1).map(p => Sample(p._1, p._2))))
    val ds2 = Dataset(Function(domain2.zip(range2).map(p => Sample(p._1, p._2))))
    val results = new PlotMerge()(ds1, ds2)
    results match {
      case Dataset(Function(it)) => {
        assertEquals(4, it.length)
        var index = 0;
        while(it.hasNext) {
          val compVal = it.next
          it.next match {
            case Sample(Tuple(tv1), Tuple(tv2)) => {
              val x = tv1(0).getNumberData.doubleValue
              val y = tv1(1).getNumberData.doubleValue
              val r1 = tv2(0).getNumberData.intValue
              val r2 = tv2(1).getNumberData.intValue.toString
              assertEquals(correct(index)(0).asInstanceOf[Double], x, 0)
              assertEquals(correct(index)(1).asInstanceOf[Double], y, 0)
              assertEquals(correct(index)(2).asInstanceOf[Int], r1, 0)
              assertEquals(correct(index)(3).asInstanceOf[String], r2)
              index += 1
            }
          }
        }
      }
    }
  }

  @Test
  def TestNearestNeighborMerge {
    val intVar = Integer(Metadata("dataVal"), 0)
    val xVar = Real(Metadata("lon"), 0.0)
    val yVar = Real(Metadata("lat"), 0.0)
    val domain1: Array[Tuple] = Array(
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.1))),
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.2))),
                                  Tuple(xVar(Data(0.1)), yVar(Data(0.3))),
                                  Tuple(xVar(Data(0.2)), yVar(Data(0.1))),
                                  Tuple(xVar(Data(0.2)), yVar(Data(0.2))),
                                  Tuple(xVar(Data(0.2)), yVar(Data(0.3))),
                                  Tuple(xVar(Data(0.3)), yVar(Data(0.1))),
                                  Tuple(xVar(Data(0.3)), yVar(Data(0.2))),
                                  Tuple(xVar(Data(0.3)), yVar(Data(0.3)))
                                )
    val domain2: Array[Tuple] = Array(
                                  Tuple(xVar(Data(0.9)), yVar(Data(0.9))),
                                  Tuple(xVar(Data(0.9)), yVar(Data(0.23))),
                                  Tuple(xVar(Data(0.23)), yVar(Data(0.9))),
                                  Tuple(xVar(Data(0.23)), yVar(Data(0.23)))
                                )
    val range1: Array[Variable] = Array(
                                  intVar(Data(1)),
                                  intVar(Data(2)),
                                  intVar(Data(3)),
                                  intVar(Data(4)),
                                  intVar(Data(5)),
                                  intVar(Data(6)),
                                  intVar(Data(7)),
                                  intVar(Data(8)),
                                  intVar(Data(9))
                                )
    val range2: Array[Variable] = Array(
                                  intVar(Data(10)),
                                  intVar(Data(11)),
                                  intVar(Data(12)),
                                  intVar(Data(13))
                                )
    val correct = Array(
                     Array(0.1, 0.1, 1, 10),
                     Array(0.1, 0.2, 2, 11),
                     Array(0.1, 0.3, 3, 11),
                     Array(0.2, 0.1, 4, 12),
                     Array(0.2, 0.2, 5, 13),
                     Array(0.2, 0.3, 6, 13),
                     Array(0.3, 0.1, 7, 12),
                     Array(0.3, 0.2, 8, 13),
                     Array(0.3, 0.3, 9, 13)
                   )
    val ds1 = Dataset(Function(domain1.zip(range1).map(p => Sample(p._1, p._2))))
    val ds2 = Dataset(Function(domain2.zip(range2).map(p => Sample(p._1, p._2))))
    val results = new NearestNeighborPlotMerge()(ds1, ds2)
    results match {
      case Dataset(Function(it)) => {
        assertEquals(9, it.length)
        var index = 0;
        while(it.hasNext) {
          val compVal = it.next
          it.next match {
            case Sample(Tuple(tv1), Tuple(tv2)) => {
              val x = tv1(0).getNumberData.doubleValue
              val y = tv1(1).getNumberData.doubleValue
              val r1 = tv2(0).getNumberData.intValue
              val r2 = tv2(1).getNumberData.intValue
              assertEquals(correct(index)(0), x, 0)
              assertEquals(correct(index)(1), y, 0)
              assertEquals(correct(index)(2), r1, 0)
              assertEquals(correct(index)(3), r2, 0)
              index += 1
            }
          }
        }
      }
    }
  }
}
