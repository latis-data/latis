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

class ImageFileWriter extends FileWriter{
  
  private var chart: JFreeChart = null
  
  def writeFile(dataset: Dataset, file: File){
    val w = new ImageWriter()
    chart = w.plotData(dataset)
    
    ChartUtilities.writeBufferedImageAsPNG(new FileOutputStream(file), chart.createBufferedImage(500, 300))
  }

}