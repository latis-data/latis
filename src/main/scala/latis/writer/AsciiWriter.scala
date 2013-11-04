package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class AsciiWriter extends Writer {
  //TODO: extend TextWriter trait, inherit some stuff
  //TODO: abstract out extensible behavior, e.g. writeHeader
  //TODO: enable writing to a String?
  //TODO: nervous about side effect issues since we are given an OutputStream

  private[this] lazy val writer = new PrintWriter(outputStream)
  
  def write(dataset: Dataset, args: Seq[String]) = {
    writer.println(dataset) //header
    writeVariable(dataset)
    writer.println()
    writer.flush()
  }
  
  def writeVariable(variable: Variable): Unit = variable match {
    //TODO: build string instead of writing directly?
    
    case Index(i)   => writer.print(i.toString)
    case Real(d)    => writer.print(d.toString)
    case Integer(l) => writer.print(l.toString)
    case Text(s)    => writer.print(s)
    
    case Tuple(vars) => {
      //surround Tuple's Variables with parens
      writer.print("(")
      //write each Variable contained in this Tuple separated by commas
      var first = true
      for (v <- vars) {
        if (!first) writer.print(",")
        else first = false
        writeVariable(v) //recursive
      }
      //close parens
      writer.print(")")
    }
    
    case f: Function => {
      writer.println()
      //TODO: "[]" brackets?
      for (Sample(domain, range) <- f.iterator) {
        writeVariable(domain)
        writer.print(" -> ")
        writeVariable(range)
        writer.println()
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