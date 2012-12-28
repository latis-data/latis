package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class AsciiWriter(out: OutputStream) extends Writer {

  private[this] val _writer = new PrintWriter(out)
  
  def write(dataset: Dataset) = {
    writeVariable(dataset)
    _writer.println()
    _writer.flush()
  }
  
  def writeVariable(variable: Variable): Unit = variable match {
    
    case Scalar(v) => _writer.print(v)
    
    case Tuple(vars) => {
      //surround Tuple's Variables with parens
      _writer.print("(")
      //write each Variable contained in this Tuple separated by commas
      var first = true
      for (v <- vars) {
        if (!first) _writer.print(",")
        else first = false
        writeVariable(v) //recursive
      }
      //close parens
      _writer.print(")")
    }
    
    case f: Function => {
      for ((domain, range) <- f.iterator) {
        writeVariable(domain)
        _writer.print(" -> ")
        writeVariable(range)
        _writer.println()
      }
    }
  }

  /**
   * Note, don't close 'out' because it was given to us.
   */
  def close() {}

}