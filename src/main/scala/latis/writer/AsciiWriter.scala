package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

/**
 * Writer to present a Dataset in a way that reflects how it is modeled.
 */
class AsciiWriter extends TextWriter {
   
  override def makeHeader(dataset: Dataset) = dataset.toString + newLine
      
  override lazy val delimiter = " -> "
  /**
   * Override to add arrow for mapping domain values to range values 
   * and to put "()" around tuple elements.
   */
  override def makeTuple(tuple: Tuple): String = {
    val delimiter = ", "
    tuple.getVariables.map(varToString(_)).mkString("(", delimiter, ")")
  }
  
  /**
   * Override so we don't call makeTuple.
   */
  override def makeSample(sample: Sample): String = sample match {
    case Sample(d, r: Function) => {
      prepend :+= varToString(d)
      varToString(r)
    }
    case Sample(d, r) => {
      val pre = if(prepend.isEmpty) "" else prepend.mkString("", delimiter, delimiter)
      val s = varToString(d) + delimiter + varToString(r)
      pre + s
    }
  }
  
}

object AsciiWriter {
  
  def apply(out: OutputStream): AsciiWriter = {
    val writer = new AsciiWriter()
    writer.setOutputStream(out)
    writer
  }
  
  def apply(): AsciiWriter = AsciiWriter(System.out)
  
  def write(dataset: Dataset) = AsciiWriter().write(dataset)
}
