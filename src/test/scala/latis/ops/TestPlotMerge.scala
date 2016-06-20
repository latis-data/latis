package latis.ops

import org.junit.Test
import org.junit.Assert._
import latis.dm._
import latis.metadata.Metadata
import latis.data.Data
import latis.writer.AsciiWriter
import latis.ops.resample.NearestNeighborResampling
import latis.metadata.Metadata

class TestPlotMerge {

  @Test
  def TestMerge {
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
    val ds1 = Dataset(Function(domain1.zip(range1).map(p => Sample(p._1, p._2))))
    val ds2 = Dataset(Function(domain2.zip(range2).map(p => Sample(p._1, p._2))))
    val results = new PlotMerge()(ds1, ds2)
    results match {
      case Dataset(Function(it)) => assertEquals(9, it.length)
    }
  }
}
