package latis.reader

import scala.io.Codec
import scala.io.Codec.ISO8859
import scala.io.Source
import scala.util.parsing.combinator.JavaTokenParsers

import latis.dm.Binary
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.Operation

class DapReader(baseUrl: String, query: String) extends DatasetAccessor {
  
  override def close = {}
  
  val codec: Codec = ISO8859
  
  lazy val dds = Source.fromURL(baseUrl + ".dds?" + query)(codec).getLines.mkString(" ")
  lazy val das = Source.fromURL(baseUrl + ".das?" + query)(codec).getLines.mkString(" ")
  lazy val data = {
    val chars = Source.fromURL(baseUrl + ".dods?" + query)(codec).drop(dds.length + "\nData:\n".length)
    //The data should be formatted in 4-byte chunks, but Source reads one byte at a time.
    //Each char read corresponds to one byte. Combine 16-bit Chars into 32-bit Ints. 
    chars.grouped(4).map(cs => (cs(0).toInt << 24) + (cs(1).toInt << 16) + (cs(2).toInt << 8) + (cs(3).toInt))
  }
  val metadata = collection.mutable.Map[String, Metadata]()
  
  lazy val origDataset = {
    DasParser.getOrigMetadata
    DdsParser.getOrigDataset
  }
  def getOrigDataset: Dataset = origDataset
  
  override def getDataset(ops: Seq[Operation]): Dataset = {
    val md = getOrigDataset.getMetadata
    val od = ops.foldLeft(getOrigDataset)((ds,op) => op(ds))
    val ov = od match {
      case Dataset(v) => v
      case _ => null
    }
    
    val v = ov match {
      //Handle the first function iteratively. The rest of the functions 
      //can't be iterative because the data must be read when the variable
      //is first seen. i.e. we can't keep an iterator that hasn't been read.
      case f: Function => { 
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
        
        Function(f, it)
      }
      
      case _ => makeVariable(ov) match {
        case Some(v) => v
        case None => ???
      }
    }
    val ds = Dataset(v, md)
    ops.foldLeft(Dataset(v, md))((ds,op) => op(ds))
  }
  
  override def getDataset: Dataset = {
    getDataset(Seq())
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
      case Some("UInt32") => Some(Integer(i.getMetadata, data.next.toLong & 0xffffffffl))
      case _ => ???
    }
    case r: Real => r.getMetadata("type") match {
      case Some("Float32") => Some(Real(r.getMetadata, java.lang.Float.intBitsToFloat(data.next)))
      case Some("Float64") => {
        val long = (data.next.toLong << 32) + 
          (data.next.toLong & 0xffffffffl) //remove leading bits for negative numbers
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
    
    Some(Function(f, it.toArray.iterator))//force reading of iterator
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
      atomic_decl  | /*array_decl |*/ structure_decl | 
      sequence_decl //| grid_decl
    
    //scalar
    def atomic_decl: Parser[Scalar] = atomic_type ~ ident <~ ";" ^^ {
      case t ~ name => {
        val md = metadata.getOrElse(name, Metadata.empty)
        t match {
          case "Byte" => Binary(md)
          case "Int16" => Integer(md + ("type"->t))
          case "UInt16" => Integer(md + ("type"->t))
          case "Int32" => Integer(md + ("type"->t))
          case "UInt32" => Integer(md + ("type"->t))
          case "Float32" => Real(md + ("type"->t))
          case "Float64" => Real(md + ("type"->t))
          case "String" => Text(md)
          case "Url" => Text(md)
        }
      }
    }
    def atomic_type = 
      "Byte" | "Int16" | "UInt16" | "Int32" | "UInt32" | 
      "Float32" | "Float64" | "String" | "Url"
    
    //index function not yet supported
//    def array_decl: Parser[Function] = 
//      array_types ~ ident ~ array_dims <~ ";" ^^ {
//      case v ~ name ~ length => {
//        val md = metadata.getOrElse(name, Metadata.empty) + ( "length" -> length)
//        Function(Index(), v, md)
//      }
//    }
//    def array_types = atomic_decl | structure_decl | sequence_decl //| grid_decl
//    def array_dims = array_dim //| array_dim ~ array_dims // only 1D arrays for now
//    def array_dim = "[" ~ ( ident ~ "=" ).? ~> wholeNumber <~ "]"
    
    //tuple
    def structure_decl: Parser[Tuple] = 
      "Structure" ~ "{" ~> structure_types.* ~ ("}" ~> ident <~ ";") ^^ {
      case vars ~ name => Tuple(vars, metadata.getOrElse(name, Metadata.empty))
    }
    def structure_types = atomic_decl | /*array_decl |*/ structure_decl | sequence_decl //| grid_decl
    
    //function
    def sequence_decl: Parser[Function] = 
      "Sequence" ~ "{" ~> sequence_types.* ~ (sequence_decl).? ~ ("}" ~> ident <~ ";") ^^ {
      case vars ~ Some(f) ~ name => {
        val md = metadata.getOrElse(name, Metadata.empty)
        vars.length match {
          case 1 => Function(vars.head, f, md)
          case _ => Function(vars.head, Tuple(vars.tail :+ f), md)
        }
      }
      case vars ~ None ~ name => {
        val md = metadata.getOrElse(name, Metadata.empty)
        vars.length match {
          case 1 => Function(Index(), vars.head, md)
          case 2 => Function(vars.head, vars.last, md)
          case _ => Function(vars.head, Tuple(vars.tail), md)
        }
      }
    }
    def sequence_types = atomic_decl | /*array_decl |*/ structure_decl //| grid_decl
    
    //not yet supported
    //def grid_decl = "grid" ~ "{" ~ "array:" ~ array_decl ~ "maps:" ~ array_decl.* ~ "}" ~ ";"
    
    def getOrigDataset = 
      parse(dds_doc, dds).get
    
  }
  
  object DasParser extends JavaTokenParsers {

    def das_doc: Parser[Unit]= "attributes {" ~ attribute_cont.* ~ "}" ^^ {case _ => }

    def attribute_decl = attribute_cont | attribute
    
    def attribute_cont: Parser[Unit] = (ident <~ "{") ~ (attribute_decl.* <~ "}") ^^ {
      case name ~ vals => {
        var md = Metadata(name)
        vals.foreach(_ match {
          case (k: String, v: String) => md += (k -> v)
          case _ => 
        })
        metadata += (name -> md)
      }
    }
    
    def attribute: Parser[(String, String)] = "string" ~> ident ~ stringLiteral <~ ";" ^^ {
      case name ~ str => (name -> str.stripPrefix("\"").stripSuffix("\""))
    }
    
    def getOrigMetadata = parse(das_doc, das).get
  }
  
}

object DapReader {
  
  def apply(url: String): DapReader = {
    //keep the base URL (without the output suffix) and query string
    url.split("""\.\w+\??""") match {
      case Array(u,q) => new DapReader(u,q)
      case Array(u) => new DapReader(u,"")
    }
  }
  
}
