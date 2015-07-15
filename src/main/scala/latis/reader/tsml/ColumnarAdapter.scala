package latis.reader.tsml

import latis.reader.tsml.ml.Tsml

/**
 * Use column index properties to parse ascii tabular data.
 * Map Variable names to zero-based column index(s).
 * Each variable spec will be separated by ";" and
 * column indices for multiple column variables will
 * be separated by ",". e.g. columns="0,1,2;5;3;4"
 */
class ColumnarAdapter(tsml: Tsml) extends AsciiAdapter(tsml) {  
  
  lazy val columnIndices: Seq[Array[Int]] = getProperty("columns") match {
    case Some(s: String) => s.split(";").map(p => p.split(",").map(_.toInt))
    case None => throw new RuntimeException("ColumnarAdapter requires 'columns' definition.")
  }
  
  override def extractValues(record: String): Seq[String] = {
    //Note, trim record first to deal with leading white space.
    val ss = record.trim.split(getDelimiter)
    if(ss.length < columnIndices.flatten.max) List() //Ignore rows with fewer columns than those requested
    else columnIndices.map(is => is.map(ss(_)).mkString(" ")) //append with " " for now since delimiter could be a regex
  }
  
}