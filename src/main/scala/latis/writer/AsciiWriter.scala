package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter

class AsciiWriter extends TextWriter {
  //TODO: rename ModelWriter?

//  private[this] lazy val writer = new PrintWriter(outputStream)
//  
//  def write(dataset: Dataset) = {
//    writer.println(dataset) //header
//    writeVariable(dataset)
//    writer.println()
//    writer.flush()
//  }
  
  override def makeHeader(dataset: Dataset) = dataset.toString
  
  def varToString(variable: Variable): String = variable match {
    //case _: Index => "" //don't include Index variable, deal with in Sample match
    case Real(d) => d.toString
    case Integer(l) => l.toString
    case Text(s) => s.trim
    case Binary(b) => "blob" //TODO: uuencode?
    //TODO: use Scalar.toStringValue?
  //TODO: deal with Time format
    
    case Sample(d, r) => varToString(d) + " -> " + varToString(r)
    case Tuple(vars) => vars.map(varToString(_)).mkString("(", delimiter, ")")
    
    case f: Function => f.iterator.map(varToString(_)).mkString(newLine)
//      (for (Sample(domain, range) <- f.iterator) 
//      yield varToString(domain) + " -> " + varToString(range)).mkString(newLine)
    // TODO: support non-flat, one row for each inner sample, repeat previous values
  }

    
//  def writeVariable(variable: Variable): Unit = variable match {
//    //TODO: build string instead of writing directly? push into super class
//    
//    case Index(i)   => writer.print(i.toString)
//    case Real(d)    => writer.print(d.toString)
//    case Integer(l) => writer.print(l.toString)
//    case Text(s)    => writer.print(s)
//    case Binary(b)  => writer.print(b) //TODO: uuencode?
//    
//    case Tuple(vars) => {
//      //surround Tuple's Variables with parens
//      writer.print("(")
//      //write each Variable contained in this Tuple separated by commas
//      var first = true
//      for (v <- vars) {
//        if (!first) writer.print(",")
//        else first = false
//        writeVariable(v) //recursive
//      }
//      //close parens
//      writer.print(")")
//    }
//    
//    case f: Function => {
//      writer.println()
//      //TODO: "[]" brackets?
//      for (Sample(domain, range) <- f.iterator) {
//        writeVariable(domain)
//        writer.print(" -> ")
//        writeVariable(range)
//        writer.println()
//      }
//    }
//  }

  /**
   * Note, don't close 'out' because it was given to us.
   * TODO: does this violate some principal? 
   *   consider java.io decorator pattern: does closing decorator close decoratee? 
   *   dangerous side effect? 
   *   use case: out from servet response, what happens if we close it here?
   */
  //def close() {}

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