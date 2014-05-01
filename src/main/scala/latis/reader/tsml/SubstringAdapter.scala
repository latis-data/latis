package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.data.Data

/**
 * Use string index properties to parse ascii tabular data.
 * The specification must be defined as a 'substring' attribute in the tsml.
 * Each Variable should have a start and stop index value separated by a comma (,).
 * These will be applied with Java's String.substring (character at final index not included).
 * each set of indices should be separated by a semi-colon (;).
 */
class SubstringAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {

  /**
   * Parse the substring indices and store by variable name.
   */
  lazy val substringIndices: Seq[Array[Int]] = getProperty("substring") match {
    case Some(s: String) => s.split(";").map(p => p.split(",").map(_.toInt))
    case None => throw new RuntimeException("SubstringAdapter requires a substring definition.")
  }
  
  /**
   * Extract the value of each Variable as a String.
   */
  override def extractValues(record: String): Seq[String] = {
    substringIndices.flatMap( is => {
      val i0 = is(0)
      val i1 = is(1)
      if (i1 > record.length) None 
      //Note, if record is too short, the size of the returned Seq will cause the superclass parseRecord to drop this record.
      //TODO: replace with fill value
      else Some(record.substring(i0, i1))
    })
  }
  
}