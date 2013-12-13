package latis.writer

import latis.dm._
import java.io.OutputStream
import java.io.PrintWriter
//import latis.time.Time
//import latis.time.TimeFormat
import java.util.Date

/**
 * Assume 1D, non-nested Function for now.
 */
class CsvWriter extends TextWriter {
  //TODO: Non-flat SSI,...
  
  //val delimiter = ","
  //TODO: get from LatisProperties
    //copy all properties for this Writer since we won't know which suffix got us here
  
  //private[this] lazy val _writer = new PrintWriter(outputStream)
  
  override def makeHeader(dataset: Dataset): String = {
    //TODO: add units
    //val header = dataset.toSeq.map(v => v.name + " (" + v.metadata("units") + ")").mkString(delimiter)
    //dataset.toSeq.map(v => v.getName).filter(_ != "index").mkString(delimiter) //don't include Index variable
    dataset.toSeq.filterNot(_.isInstanceOf[Index]).map(v => v.getName).mkString("", delimiter, newLine) //don't include Index variable
  }
  
  def varToString(variable: Variable): String = variable match {
    //case _: Index => "" //don't include Index variable, deal with in Sample match
    case Real(d) => d.toString
    case Integer(l) => l.toString
    case Text(s) => s.trim
    case Binary(b) => "NaN" //TODO: uuencode?
    //TODO: use Scalar.toStringValue?
  //TODO: deal with Time format
    
    case Sample(d: Index, r) => varToString(r) //drop Index domain
    case Tuple(vars) => vars.map(varToString(_)).mkString(delimiter)
    
    case f: Function => f.iterator.map(varToString(_)).mkString(delimiter)
    // TODO: support non-flat, one row for each inner sample, repeat previous values
  }
  
//  def write(dataset: Dataset) = {
//    
//    val header = makeHeader(dataset)
//    _writer.println(header)
//    
//    for (v <- dataset.getVariables) {
//      v match {
//        
//        case f: Function => {
//          for (Sample(domain, range) <- f.iterator) {
//            val sb = new StringBuilder()
//            /*
//             *
//             * 
//             */
//          
// //           if (! domain.isInstanceOf[Index]) record ++= buildRecord(domain) //don't include Index domain
//            sb append buildRecord(domain)
//            sb append buildRecord(range)
//            val record = sb.take(sb.length - delimiter.length) //drop trailing delimiter
//            _writer.println(record)
//          }
//        }
//        
//        case _ => {
//          var record = buildRecord(v)
//          record = record.substring(0, record.length - delimiter.length) //drop trailing delimiter
//          _writer.println(record)
//        }
//      }
//    }
//
//    _writer.flush()
//  }
//  
//  /**
//   * Construct the String representation of the given Variable.
//   * The result will include a trailing delimiter.
//   */
//  def buildRecord(variable: Variable): String = variable match {
//    
//    //TODO: handle time format better, use general getStringValue? see TSS1
//    //  units vs format?
////    case Scalar(v) => {
//////      if (variable.isInstanceOf[Time]) variable.metadata.get("format") match {
//////        case Some(format) => variable.asInstanceOf[Time].format + delimiter
//////        case None => v + delimiter
//////      }
//////      else 
////        v + delimiter
////    }
//    
//    //Don't print Index
//    case _: Index => ""
//    
//    case Real(d) => d.toString + delimiter
//    
//    case Integer(l) => l.toString + delimiter
//    
//    case Text(s) => s.trim + delimiter
//    
//    case Tuple(vars) => {
//      val sb = new StringBuilder
//      for (v <- vars) sb.append(buildRecord(v))
//      sb.toString
//    }
//    
//    case f: Function => {
//      //Inner Function, just write range values, "flattened"
//      val sb = new StringBuilder
//      for (Sample(_,r) <- f.iterator) sb.append(buildRecord(r))
//      sb.toString
//    }
//    
//    case null => ""
//  }

}