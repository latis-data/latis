package latis.reader

import scala.util.parsing.combinator.JavaTokenParsers
import latis.dm._
import latis.dm.Binary
import latis.metadata.Metadata
import scala.io.Source

class DapReader(url: String) {
  
  val dds = Source.fromURL(url + ".dds").getLines.mkString(" ")
  val data = {
    val chars = Source.fromURL(url + ".dods").drop(dds.length + "\nData:\n".length)
    //The data should be in 4-byte chunks, but Source reads in 1-byte chunks.
    //Each char read corresponds to one byte. Combine 16-bit Chars into 32-bit Ints. 
    chars.grouped(4).map(cs => (cs(0).toInt << 24) + (cs(1).toInt << 16) + (cs(2).toInt << 8) + (cs(3).toInt))
  }
  
  lazy val origDataset = DdsParser.getDataset(dds)
  def getOrigDataset: Dataset = origDataset
  
  def getDataset: Dataset = {
    val md = getOrigDataset.getMetadata
    val ov = getOrigDataset.unwrap
    
    makeVariable(ov) match {
      case Some(v) => Dataset(v, md)
      case None => Dataset.empty
    }
    
  }
  
  def makeVariable(v: Variable): Option[Variable] = v match {
    case s: Scalar => makeScalar(s)
    case s: Sample => makeSample(s)
    case t: Tuple => makeTuple(t)
    case f: Function => makeFunction(f)
  }
  
  def makeScalar(s: Scalar): Option[Scalar] = s match {
    case i: Integer => i.getMetadata("type") match { //match type to preserve sign
      case Some("Int16") => Some(Integer(i.getMetadata, data.next.toShort))
      case Some("UInt16") => Some(Integer(i.getMetadata, data.next))
      case Some("Int32") => Some(Integer(i.getMetadata, data.next))
      case Some("UInt32") => Some(Integer(i.getMetadata, data.next.toLong))
      case _ => ???
    }
    case r: Real => r.getMetadata("type") match {
      case Some("Float32") => Some(Real(r.getMetadata, java.lang.Float.intBitsToFloat(data.next)))
      case Some("Float64") => {
        val long = (data.next.toLong << 32) + data.next.toLong
        Some(Real(r.getMetadata, java.lang.Double.longBitsToDouble(long)))
      }
      case _ => ???
    }
    case t: Text => {
      val length = data.next
      val md = t.getMetadata + ("length" -> length.toString)
      //all the byte chunks that contain this string
      val ints = data.next +: Seq.fill((length-1)/4)(data.next)
      //break the ints into bytes
      val bytes = ints.flatMap(i => Seq.tabulate(4)(n => (i >> (n * 8)).toByte).reverse)
      
      val str: String = bytes.map(_.toChar).mkString
      
      Some(Text(md, str))
    }
  }
  
  def makeSample(s: Sample): Option[Sample] = {
    val dom = makeVariable(s.domain)
    val ran = makeVariable(s.range)
    (dom,ran) match {
      case (Some(d), Some(r)) => Some(Sample(d, r))
      case _ => ???
    }
  }
  
  def makeTuple(t: Tuple): Option[Tuple] = {
    val vars = t.getVariables.flatMap(makeVariable(_))
    Some(Tuple(vars, t.getMetadata))
  }
  
  def makeFunction(f: Function): Option[Function] = {
    //TODO: Array
    val START_OF_INSTANCE = 0x5a000000
    val END_OF_SEQUENCE = 0xa5000000
    
    val it = new Iterator[Sample] {
      def hasNext = {
        val n = data.next
        (n != END_OF_SEQUENCE) && (n == START_OF_INSTANCE)
      }
      def next = makeVariable(f.getSample) match {
        case Some(s: Sample) => s
        case _ => next
      }
    }
    
    Some(Function(f, it))
  }
  
  
  object DdsParser extends JavaTokenParsers {
     
    //dataset
    def dds_doc: Parser[Dataset] = 
      "Dataset" ~ "{" ~> type_decl.* ~ ("}" ~> ident <~ ";") ^^ {
        case vars ~ name => vars.length match {
          case 0 => Dataset.empty
          case 1 => Dataset(vars.head, Metadata(name))
          case _ => Dataset(Tuple(vars), Metadata(name))
        }
    }
    def type_decl: Parser[Variable] = 
      atomic_decl  | array_decl | structure_decl | 
      sequence_decl //| grid_decl
    
    //scalar
    def atomic_decl: Parser[Scalar] = atomic_type ~ ident <~ ";" ^^ {
      case t ~ name => t match {
        case "Byte" => Binary(Metadata(name))
        case "Int16" => Integer(Metadata("name"->name, "type"->t))
        case "UInt16" => Integer(Metadata("name"->name, "type"->t))
        case "Int32" => Integer(Metadata("name"->name, "type"->t))
        case "UInt32" => Integer(Metadata("name"->name, "type"->t))
        case "Float32" => Real(Metadata("name"->name, "type"->t))
        case "Float64" => Real(Metadata("name"->name, "type"->t))
        case "String" => Text(Metadata(name))
        case "Url" => Text(Metadata(name))
      }
    }
    def atomic_type = 
      "Byte" | "Int16" | "UInt16" | "Int32" | "UInt32" | 
      "Float32" | "Float64" | "String" | "Url"
    
    //index function
    def array_decl: Parser[Function] = 
      array_types ~ ident ~ array_dims <~ ";" ^^ {
      case v ~ name ~ length => {
        val md = Metadata(Map("name" -> name, "length" -> length))
        Function(Index(), v, md)
      }
    }
    def array_types = atomic_decl | structure_decl | sequence_decl //| grid_decl
    def array_dims = array_dim //| array_dim ~ array_dims // only 1D arrays for now
    def array_dim = "[" ~ ( ident ~ "=" ).? ~> wholeNumber <~ "]"
    
    //tuple
    def structure_decl: Parser[Tuple] = 
      "Structure" ~ "{" ~> structure_types.* ~ ("}" ~> ident <~ ";") ^^ {
      case vars ~ name => Tuple(vars, Metadata(name))
    }
    def structure_types = atomic_decl | array_decl | structure_decl | sequence_decl //| grid_decl
    
    //function
    def sequence_decl: Parser[Function] = 
      "Sequence" ~ "{" ~> sequence_types.* ~ (sequence_decl).? ~ ("}" ~> ident <~ ";") ^^ {
      case vars ~ Some(f) ~ name => vars.length match {
        case 1 => Function(vars.head, f, Metadata(name))
        case _ => Function(vars.head, Tuple(vars.tail :+ f), Metadata(name))
      }
      case vars ~ None ~ name => vars.length match {
        case 1 => Function(Index(), vars.head, Metadata(name))
        case 2 => Function(vars.head, vars.last, Metadata(name))
        case _ => Function(vars.head, Tuple(vars.tail), Metadata(name))
      }
    }
    def sequence_types = atomic_decl | array_decl | structure_decl //| grid_decl
    
    //not yet supported
    //def grid_decl = "grid" ~ "{" ~ "array:" ~ array_decl ~ "maps:" ~ array_decl.* ~ "}" ~ ";"
    
    def getDataset(dds: String) = 
      parse(dds_doc, dds).get
    
  }
  
}