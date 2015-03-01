package latis.writer

import latis.dm.Dataset
import latis.dm.Scalar
import latis.dm.Function
import latis.dm.Tuple
import latis.dm.Variable

import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartUtilities
import org.jfree.chart.JFreeChart
import org.jfree.data.xy.XYSeries
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.chart.axis._
import java.io.File
import java.io.FileOutputStream
import org.jfree.data.time.TimeSeriesCollection
import org.jfree.data.time.TimeSeries
import org.jfree.data.time.TimeSeriesDataItem
import org.jfree.data.time.Millisecond
import java.util.Date
import org.jfree.data.time.Day
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import java.awt.Color

  /**
   * Writes a line graph of the data to the output stream. 
   * The dataset must have a scalar independent variable and a scalar or tuple dependent variable.
   */
class ImageWriter extends FileWriter{
  
  private var chart: JFreeChart = null
  private var plot: XYPlot = new XYPlot
  private var plotIndex = 0
  
  def writeFile(dataset: Dataset, file: File) {
    plotDataset(dataset)
    if(plotIndex>3) plot.setFixedRangeAxisSpace(new AxisSpace)
    chart = new JFreeChart(dataset.getMetadata.getOrElse("long_name", dataset.getName), plot)
    ChartUtilities.writeBufferedImageAsPNG(new FileOutputStream(file), chart.createBufferedImage(500, 300))
  }

  def plotDataset(dataset: Dataset) {
    dataset.unwrap match {
      case function: Function => {
        val x = function.getDomain
        if (x.isInstanceOf[latis.time.Time] && x.getMetadata("type").get == "text") {
          val axis = new DateAxis(x.getName)
          plot.setDomainAxis(axis)
        } else {
          val axis = new NumberAxis(x.getName)
          axis.setAutoRangeIncludesZero(false)
          plot.setDomainAxis(axis)
        }
        val y = function.getRange
        plotVariable(x, y, dataset.toDoubleMap)
      }
      
      case _ => throw new Error("Dataset has no Function to plot.")
    }
  }

  def plotVariable(x: Variable, y: Variable, data: scala.collection.Map[String,Array[Double]]) {
    y match{
      case t: Tuple => plotTuple(x, t, data)
      case s: Scalar => plotScalar(x, s, data)
    }
  }
  
  def plotTuple(x: Variable, y: Tuple, data: scala.collection.Map[String,Array[Double]]) {
    for(b <- y.getVariables) plotVariable(x, b, data)
  }

  def plotScalar(x: Variable, y: Scalar, data: scala.collection.Map[String,Array[Double]]) {
    val col = new XYSeriesCollection
    col.addSeries(makeSeries(x, y, data))
    val axis = new NumberAxis(y.getName)
    axis.setAutoRangeIncludesZero(false)
    val rend = new XYLineAndShapeRenderer(true, false)
    plot.setRenderer(plotIndex, rend)
    plot.setDataset(plotIndex, col)
    plot.setRangeAxis(plotIndex, axis)
    plot.mapDatasetToRangeAxis(plotIndex,plotIndex)
    plotIndex = plotIndex + 1
  }
  
  def makeSeries(a: Variable, b: Scalar, data: scala.collection.Map[String,Array[Double]]): XYSeries = {
    val mv = b.getMetadata.get("missing_value").getOrElse("") 
    val x = data(a.getName)
    val y = data(b.getName)
    val series = new XYSeries(b.getName)
    for(i <- 0 until x.length) {
      if (mv=="")
        series.add(x(i),y(i))
      else if (y(i)!=mv.toDouble)
        series.add(x(i),y(i)) 
    }
    series
  }
  
  override def mimeType: String = "image/png" 
}
