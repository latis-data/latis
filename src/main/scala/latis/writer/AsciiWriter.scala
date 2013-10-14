package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class AsciiWriter extends Writer {
  //TODO: extend TextWriter trait, inherit some stuff
  //TODO: abstract out extensible behavior, e.g. writeHeader
  //TODO: enable writing to a String?
  //TODO: nervous about side effect issues since we are given an OutputStream

  private[this] lazy val _writer = new PrintWriter(outputStream)
  
  def write(dataset: Dataset, args: Seq[String]) = {
    _writer.println(dataset) //header
    writeVariable(dataset)
    _writer.println()
    _writer.flush()
  }
  
  def writeVariable(variable: Variable): Unit = variable match {
    //TODO: build string instead of writing directly?
    
    //case Scalar(v) => _writer.print(v)
    case Index(i) => _writer.print(i.toString)
    case Real(d) => _writer.print(d.toString)
    case Integer(l) => _writer.print(l.toString)
    case Text(s) => _writer.print(s)
    
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
      _writer.println()
      for (Sample(domain, range) <- f.iterator) {
        writeVariable(domain)
        _writer.print(" -> ")
        writeVariable(range)
        _writer.println()
      }
    }
  }

  /**
   * Note, don't close 'out' because it was given to us.
   * TODO: does this violate some principal? 
   *   consider java.io decorator pattern: does closing decorator close decoratee? 
   *   dangerous side effect? 
   *   use case: out from servet response, what happens if we close it here?
   */
  def close() {}

}

object AsciiWriter {
  
  def apply(out: OutputStream) = {
    val writer = new AsciiWriter()
    writer._out = out
    writer
  }
  
  def apply() = {
    val writer = new AsciiWriter()
    writer._out = System.out
    writer
  }
  
  def write(dataset: Dataset) = AsciiWriter().write(dataset)
}