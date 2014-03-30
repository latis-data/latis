package latis.writer

import latis.dm._
import org.jfree.chart.JFreeChart
import org.jfree.chart.ChartUtilities
import org.jfree.chart.ChartFactory
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.data.xy.XYSeries
import java.io.File

  /**
   * Writes a line graph of the data to the output stream. 
   * The dataset must have a scalar independent variable and a scalar or tuple dependent variable.
   */
class ImageWriter extends Writer{
  
  private var chart: JFreeChart = null
  
  def write(dataset: Dataset) {
    plotData(dataset)
    ChartUtilities.writeBufferedImageAsPNG(getOutputStream, chart.createBufferedImage(500, 300))
  }

  def plotData(dataset: Dataset) {
    val function = dataset.findFunction.get
    val a = function.getDomain
    val b = function.getRange
    plotFunction(a, b, dataset)
    fixRange(chart)
  }

  def fixRange(chart: JFreeChart) {
    val plot = chart.getXYPlot
    val range = plot.getRangeAxis
    range.setRangeWithMargins(plot.getDataRange(plot.getRangeAxis()))
  }

  def makeSeries(a: Variable, b: Variable, data: scala.collection.Map[String,Array[Double]]): XYSeries = {
    val x = data(a.getName)
    val y = data(b.getName)
    val series = new XYSeries(b.getName)
    for(i <- 0 until x.length) series.add(x(i),y(i))
    series
  }

  def plotFunction(x: Variable, y: Variable, dataset: Dataset) {
    val data = latis.util.DataMap.toDoubleMap(dataset)
    val xycol = new XYSeriesCollection()
    y match{
      case _:Tuple => for (b <- y.getVariables) xycol.addSeries(makeSeries(x, b, data))
      case _:Scalar => xycol.addSeries(makeSeries(x, y, data))
    }
    chart = ChartFactory.createXYLineChart(dataset.getName, x.getName, y.getName, xycol, org.jfree.chart.plot.PlotOrientation.VERTICAL, true, false, false)
  }
  
  override def mimeType: String = "image/png" 
}