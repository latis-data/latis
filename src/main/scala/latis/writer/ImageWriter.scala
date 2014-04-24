package latis.writer

import latis.dm._
import org.jfree.chart.JFreeChart
import org.jfree.chart.ChartUtilities
import org.jfree.chart.ChartFactory
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.data.xy.XYSeries
import org.jfree.chart.axis._
import java.io.File
import java.io.FileOutputStream

  /**
   * Writes a line graph of the data to the output stream. 
   * The dataset must have a scalar independent variable and a scalar or tuple dependent variable.
   */
class ImageWriter extends FileWriter{
  
  private var chart: JFreeChart = null
  
  def writeFile(dataset: Dataset, file: File) {
    plotData(dataset)
    ChartUtilities.writeBufferedImageAsPNG(new FileOutputStream(file), chart.createBufferedImage(500, 300))
  }

  def plotData(dataset: Dataset): JFreeChart = {
    val function = dataset.findFunction.get
    val a = function.getDomain
    val b = function.getRange
    plotFunction(a, b, dataset)
    fixRange(chart)
    chart
  }

  def fixRange(chart: JFreeChart) {
    val plot = chart.getXYPlot
    val range = plot.getRangeAxis
    //range.setRangeWithMargins(plot.getDataRange(plot.getRangeAxis()))
    range match{
      case a:NumberAxis=>a.setAutoRangeIncludesZero(false)
    }
    //range.setAutoRangeIncludesZero(false)
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