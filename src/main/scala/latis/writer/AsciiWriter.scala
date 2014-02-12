package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class AsciiWriter extends TextWriter {
  //TODO: rename ModelWriter?
  
  private var indent = 0
  
  override def makeHeader(dataset: Dataset) = dataset.toString + newLine
    
  /**
   * Override to add arrow for mapping domain values to range values 
   * and to put "()" around tuple elements.
   */
  override def makeTuple(tuple: Tuple): String = tuple match {
    case Sample(d, r) => varToString(d) + " -> " + varToString(r)
    case Tuple(vars) => vars.map(varToString(_)).mkString("(", delimiter, ")")
  }
  
  /**
   * Override to indent nested function samples.
   */
  override def makeFunction(function: Function): String = {
    indent += 5
    val s = function.iterator.map(varToString(_)).mkString(newLine + " "*indent)
    indent -= 5
    s
  }

}

object AsciiWriter {
  
  def apply(out: OutputStream): AsciiWriter = {
    val writer = new AsciiWriter()
    writer._out = out
    writer
  }
  
  def apply(): AsciiWriter = AsciiWriter(System.out)
  
  def write(dataset: Dataset) = AsciiWriter().write(dataset)
}