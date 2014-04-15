package latis.reader.tsml

import latis.dm._
import scala.xml.{Elem,Node}
import scala.collection._
import latis.metadata._
import javax.naming.directory.Attributes
import latis.ops.Operation
import latis.time.Time
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import java.io.File
import latis.reader.tsml.ml.VariableMl
import latis.reader.tsml.ml.ScalarMl
import latis.reader.tsml.ml.FunctionMl
import latis.reader.tsml.ml.TupleMl
import latis.reader.tsml.ml.Tsml
import java.net.URL
import latis.ops.Projection
import latis.ops.filter.Selection
import latis.data._
import latis.data.EmptyData
import java.nio.ByteBuffer
import scala.io.Source
import java.net.URI


/**
 * Base class for Adapters that read dataset as defined by TSML.
 * The "dsml" constructor argument is single "dataset" child XML 
 * element of the tsml element.
 */
abstract class TsmlAdapter(val tsml: Tsml) {
  
  //---- Adapter properties from TSML -----------------------------------------
  
  /**
   * Store XML attributes for this Adapter definition as a properties Map.
   */
  private val properties: Map[String,String] = tsml.dataset.getAdapterAttributes()

  /**
   * Return Some property value or None if property does not exist.
   */
  def getProperty(name: String): Option[String] = properties.get(name)
  
  /**
   * Return property value or default if property does not exist.
   */
  def getProperty(name: String, default: String): String = getProperty(name) match {
    case Some(v) => v
    case None => default
  }
  
  //---- Original Dataset Construction ----------------------------------------
  
  /**
   * The original Dataset as defined by the TSML.
   * This will only include Data values that are defined in the TSML.
   */
  private lazy val origDataset: Dataset = makeOrigDataset
  def getOrigDataset = origDataset
  
  /**
   * Keep a list of the original Scalars.
   */
  private lazy val origScalars = origDataset.toSeq
  def getOrigScalars = origScalars
  
  /**
   * Keep a list of the names of the original Scalars.
   * Don't include "index" which is only a place holder when there is no other domain variable.
   */
  private lazy val origScalarNames = origScalars.map(_.getName).filter(_ != "index")
  def getOrigScalarNames = origScalarNames
  
  
  protected def makeOrigDataset: Dataset = {
    val md = makeMetadata(tsml.dataset)
    val vars = tsml.dataset.getVariableMl.flatMap(makeOrigVariable(_))
    Dataset(vars, md) 
  } 
  
  /**
   * Create Metadata from "metadata" elements or Variable element's attributes
   * in the given Variable XML.
   */
  protected def makeMetadata(vml: VariableMl): Metadata = {
    //Note, not recursive, each Variable's metadata is independent
    //TODO: could this be private? AggregationAdapter overrides it
    
    //Add tsml attributes for the variable element to attributes from the metadata element.
    var atts = vml.getAttributes ++ vml.getMetadataAttributes
    
    //internal helper method to add default name for special variable types
    def addImplicitName(name: String) = {
      //If the Variable already has a name, add the given name as an alias
      if (atts.contains("name")) atts.get("alias") match {
        case Some(a) => atts = atts + ("alias" -> (a+","+name)) //append to list of existing aliases
        case None => atts = atts + ("alias" -> name) //add alias
      } 
      else atts = atts + ("name" -> name) //no 'name' attribute, so use it
    }
 
    //Add implicit metadata for "time" and "index" variables.
    //TODO: need to make them unique? consider finding "time" variable, finds first
    //  do only when time domain is implied?
    if (vml.label == "time") addImplicitName("time")
    if (vml.label == "index") addImplicitName("index")

    Metadata(atts)
  }
    
  //this will be used only by top level Variables (or kids of top level Tuples)
  //TODO: do we need Option since we are only building what tsml defines?
  private def makeOrigVariable(vml: VariableMl): Option[Variable] = {
    //TODO: support Data values defined in tsml
    val md = makeMetadata(vml)
    vml match {
      case sml: ScalarMl => Some(Scalar(sml.label, md))
      case tml: TupleMl  => Some(Tuple(tml.variables.flatMap(makeOrigVariable(_)), md))
      case fml: FunctionMl => for (domain <- makeOrigVariable(fml.domain); range <- makeOrigVariable(fml.range)) 
        yield Function(domain, range, md)
    }
  }
  

  //---- Dataset Construction -------------------------------------------------
  
  /*
   * Apply the given sequence of operations and return the resulting Dataset.
   * This will also apply the TSML Processing Instructions.
   * This will trigger the construction of an original Dataset based
   * only on the tsml, without these constraints applied.
   * Adapters can override this to apply operations more effectively during
   * the Dataset construction process (e.g. use in SQL query).
   * 
   */
  //TODO: save dataset for reuse?
  //TODO: use this dataset as cache? data with PIs applied but not user ops,
//TODO: Note that these are no longer Tsml specific! could we use them elsewhere? 
  //e.g. ProjectedFunction makeSample, but we do count on special adapters overriding these
  
  /**
   * Hook for subclasses to do something before constructing the Dataset.
   * Note, this happens after the first Dataset construction pass (loading TSML).
   */
  def init: Unit = {}
  
  /**
   * The final Dataset that this Adapter produces.
   */
  private lazy val dataset: Dataset = {
    init
    makeDataset(origDataset)
  }
  
  def getDataset: Dataset = dataset
  
  def getDataset(ops: Seq[Operation]): Dataset = {
    
    //Use handleOps to let adapter handle some and leave rest for here
    //TODO: what can we do about preserving operation order if we let adapters handle what they want?
    //  require adapter to override then be responsible for applying all ops?
    //  what about leaving some for the writer? wrap in "write(format="",...)" function?
    //  Note, PIs already processed, consider rename PI breaking sql...
    
    //Combine data provider processing instructions with user ops.
    //Give the adapter the opportunity to handle them. 
    val others = (piOps ++ ops).filterNot(handleOperation(_))
    
    //Apply operations that the adapter didn't handle.
    //Reverse because foldRight applies them in reverse order.
    //This may be the first use of the lazy 'dataset' so it may trigger its final construction.
    others.reverse.foldRight(getDataset)(_(_))
  }
  
  //TODO: "build" vs "make"? consider scala Builder
  
  protected def makeDataset(ds: Dataset): Dataset = {
    val vars = ds.getVariables.flatMap(makeVariable(_))
    Dataset(vars, ds.getMetadata) 
  } 
  
  //lots of extension points
  
  protected def makeVariable(variable: Variable): Option[Variable] = variable match {
    case scalar:   Scalar   => makeScalar(scalar)
    case sample:   Sample   => makeSample(sample)
    case tuple:    Tuple    => makeTuple(tuple)
    case function: Function => makeFunction(function)
  }
  
  //default to no-op
  //TODO: deal with data defined in tsml
  protected def makeScalar(scalar: Scalar): Option[Scalar] = Some(scalar)
//  {
//    scalar.getData match {
//      case EmptyData => Some(scalar) //no need for modified copy
//      //case d: Data => Some(Scalar(scalar.getMetadata, d)) //make new Scalar with md and data from orig
//      //TODO: make sure we get the same type, use Builder pattern? CanBuildFrom? 
//      //copyWithNewData?
//    }
//  }
  
  protected def makeSample(sample: Sample): Option[Sample] = {
    //Note this uses a -1 place holder for Index.
    //ProjectedFunction will set the appropriate index value
    val odomain = makeVariable(sample.domain)
    val orange  = makeVariable(sample.range)
    (odomain, orange) match {
      case (Some(d), Some(r)) => Some(Sample(d,r))
      case (None, Some(r))    => Some(Sample(Index(-1), r)) //no domain, so replace with Index. 
      case (Some(d), None)    => Some(Sample(Index(-1), d)) //no range, so make domain the range of an index function
      case (None, None)       => None //nothing projected
    }
  }
  
  
  protected def makeTuple(tuple: Tuple): Option[Tuple] = {
    val md = tuple.getMetadata
    val vars = tuple.getVariables.flatMap(makeVariable(_)) 
    vars.length match {
      case 0 => None
      case n => Some(Tuple(vars, md))
      //TODO: make scalar if only one variable?
    }
  }
  
  protected def makeFunction(function: Function): Option[Function] = {
    //TODO: use Builder? Variable.build[T](template: Variable): T  CanBuildFrom...  builder += (elem), don't want to do it one elem at a time
    //  buildWith(template, metadata, data)?
    val md = function.getMetadata
    //TODO: if domain or range None, use IndexFunction
    //  where else is this handled? makeSample above and ProjectionFunction
    //  do we need to worry about makeVar returning None here?
    //  this case assumes kids with data, if one is missing replace with Index.withLength
    
    //TODO: function may have _iterator already (e.g. agg)
    
    
    
    
    
    for (domain <- makeVariable(function.getDomain); 
         range  <- makeVariable(function.getRange)
    ) yield Function(domain, range, md)
  }
  /*
   * TODO: consider how Iterative vs Granule adapters should handle extension
   * traits that override makeFunction?
   * but no state for data map...
   * would like to have FooAdapter with either mixed-in, but probably no do-able
   * do at Data level?
   *   DataGranule: column-oriented, Map, Data for each Scalar
   *   DataIterator: ByteBufferData with sample size, Data in Function
   */
  
  
  /**
   * Hook for subclasses to apply operations during data access
   * to reduce data volumes. (e.g. query constraints to database or web service)
   * Return true if it will be handled. Otherwise, it will be applied
   * to the Dataset by the TsmlAdapter superclass.
   * The default behavior is for the Adapter subclass to handle no operations.
   */
  def handleOperation(op: Operation): Boolean = false 
  
  
  /**
   * Get the TSML Processing Instructions as a Seq of Operations.
   */
  def piOps: Seq[Operation] = {
    val projections = tsml.getProcessingInstructions("project").map(Projection(_)) //TODO: do these need to be last?
    val selections  = tsml.getProcessingInstructions("select").map(Selection(_))
    projections ++ selections
  }
  
  /**
   * Find a Selection among the given Operations for the given variable name.
   */
  def findSelection(ops: Seq[Operation], name: String): Option[Selection] = {
    ops.filter(_.isInstanceOf[Selection]).map(_.asInstanceOf[Selection]).find(_.vname == name)
  }
  //TODO: ByName and ByType?

  //-------------------------------------------------------------------------//

  /*
   * TODO: do we need diff place to hold tsml values so we don't have partial cache confusion?
   * allow mutable and require getCachedData? hard to enforce
   * use tsmlData?
   */
  //val tsmlData = Map[String, Data]() //TODO: immutable?
  
  /**
   * Column-oriented data cache.
   */
  private val dataCache = mutable.Map[String, Data]()
  
  //TODO: consider mutability issues
  //TODO: consider ehcache
  
  /**
   * Add Data to the cache.
   */
  protected def cache(data: Map[String, Data]) = dataCache ++= data
  
  /**
   * Get the Data that has been cached for the given variable.
   */
  def getCachedData(variableName: String): Option[Data] = dataCache.get(variableName)
  //TODO: if None throw new Error("No data found in cache for Variable: " + variableName)? or return empty Data?

  
  //-------------------------------------------------------------------------//
  
  private var source: Source = null
  
  /**
   * Source from which we will read data.
   */
  def getDataSource: Source = {
    if (source == null) source = Source.fromURL(getUrl())
    source
  }
    
    
  
  /**
   * Get the URL of the data source from this adapter's definition.
   */
  def getUrl(): URL = {
    //Note, can't be relative to the tsml file since we only have xml here. Tsml could be from any source.
    properties.get("location") match {
      case Some(loc) => {
        val uri = new URI(loc)
        if (uri.isAbsolute) uri.toURL //starts with "scheme:...", note this could be file, http, ...
        else if (loc.startsWith(File.separator)) new URL("file:" + loc) //absolute path
        else getClass.getResource("/"+loc) match { //relative path: try looking in the classpath
          case url: URL => url
          case null => new URL("file:" + scala.util.Properties.userDir + File.separator + loc) //relative to current working directory
        }
      }
      case None => throw new RuntimeException("No 'location' attribute in TSML adapter definition.")
    }
  }
  
//  /**
//   * Get the URL of the data source from this adapter's definition.
//   */
//  def getUrl(): String = {
//    //Note, can't be relative to the tsml file since we only have xml here. Tsml could be from any source.
//    properties.get("location") match {
//      case Some(loc) => {
//        //TODO: use URI API?
//        if (loc.contains(":")) loc //Assume URL is absolute (has scheme) if ":" exists.
//        else if (loc.startsWith(File.separator)) "file:" + loc //full path
//        else getClass.getResource("/"+loc) match { //try in the classpath
//          case url: URL => url.toString
//          case null => "file:" + scala.util.Properties.userDir + File.separator + loc //relative to current working directory
//        }
//      }
//      case None => throw new RuntimeException("No 'location' attribute in TSML adapter definition.")
//    }
//  }
  
  def close() {
    if (source != null) source.close
  }
  
  //=================================================================================================


//  
//  /**
//   * Cache for Variables defined with "values".
//   * Such Variables may be used elsewhere by variable definitions that have the "ref" attribute.
//   * The "ref" attribute must match the "name" attribute which is the key for this cache.
//   */
//  lazy val refCache = new HashMap[String,Variable]()

//  
//  /**
//   * Construct the Metadata for the Dataset and it's Variables.
//   * Cache any variables that have values defined in the TSML.
//   */  
//  private def makeMetadata(elem: Elem): Option[Metadata] = {
//    val props = getProperties(elem)
//    
//    elem match {
//      
//      case <dataset>{ns @ _*}</dataset> => {
//        //If there is a top level "time" element, make a time series Function to wrap everything
//        //with all following variables as the range.
//        //Require that the time element is the first.
//        
//        //Keep only the element nodes as a seq of Elem, drop the adapter definition
//        val es = getElements(ns).tail
//        
//        if (es.head.label == "time") {
//          //implicit time series Function
//          //TODO: allow previous nodes (e.g. SSI wavelengths)
///*
// * TODO: support values, ref for time
// */       
//          val timeMd = ScalarMd(getProperties(es.head) ++ Time.defaultMd) //TODO: allow diff name and subtype?
//          val rangeMd = TupleMd(HashMap[String,String](), es.tail.flatMap(makeMetadata(_))).flatten() //TODO: add some metadata
//          val tsmd = FunctionMd(HashMap[String,String](), timeMd, rangeMd) //TODO: add some metadata
//          Some(TupleMd(props, Seq(tsmd)))
//        } else {
//          Some(TupleMd(props, es.flatMap(makeMetadata(_))))
//        }
//      }
//      
//      case <scalar>{ns @ _*}</scalar> => {
//        //If this scalar is a ref, copy the metadata from the ref'd scalar.
//        //TODO: allow additional attributes to be defined in this ref and merge?
//        if (props.contains("ref")) {
//          val refname = props("ref")
//          refCache.get(refname) match {
//            case Some(v: Variable) => {
//              //Add the metadata properties form the ref'd variable
//              //TODO: assuming that the ref'd variable is an index function defined as a scalar, just get the range for the md
//              //TODO: keeping the "ref" for now so the data parser knows
//              val mdref = v.metadata.asInstanceOf[FunctionMd].range
//              val md = ScalarMd(props ++ mdref.properties)
//              Some(md)
//            }
//            case None => throw new RuntimeException("No Variable found for reference: " + refname)
//          }
//        } else {
//          val md = ScalarMd(props)
//          //If values are defined, make the variable and cache it
///*
// * TODO: allow definition of values for tuples and functions AND time
// * No, tuple and function still need scalar defs within them, put values there
// * just use text content instead of a "values" element?
// * consider "metadata" elements
// */
//
//          ns.find(_.label == "values") match {
//            case Some(elem: Elem) => {
//              makeVariableFromValues(md, elem) match {
//                case Some(v) => refCache += ((md.name, v)) //add variable to the cache
//                case None => throw new RuntimeException("Unable to make Variable from values for " + md.name)
//              }
//              //exclude from model, use by ref only
//              //TODO: look for processing instruction
//              md.get("exclude") match {
//                case Some(s) if (s.toLowerCase() == "true") => None
//                case _ => Some(md) //keep the variable with defined values in the model
//              }
//            }
//            case None => Some(md) //no values defined
//          }
//          
//        }
//      }
//      
//      case <tuple>{ns @ _*}</tuple>     => Some(TupleMd(props, getElements(ns).flatMap(makeMetadata(_))))
//      case <domain>{ns @ _*}</domain>   => Some(TupleMd(props, getElements(ns).flatMap(makeMetadata(_))).flatten())
//      case <range>{ns @ _*}</range>     => Some(TupleMd(props, getElements(ns).flatMap(makeMetadata(_))).flatten())
//      
//      case <function>{ns @ _*}</function> => {
//        val es = getElements(ns)
//        //Look for domain and range definitions.
//        if (es.head.label == "domain") {
//          val domain = makeMetadata(es.head)
//          //TODO: assert that there is only one other element that is "range"
//          val range = makeMetadata(es.tail.head)
//          //TODO: deal with Option, error if None
//          Some(FunctionMd(props, domain.get, range.get))
//        } else {
//          //No domain defined. Assume the first variable is the domain and the rest are the range.
//          val domain = makeMetadata(es.head).get 
//          val range = TupleMd(HashMap[String,String](), es.tail.flatMap(makeMetadata(_))).flatten() //TODO: add some metadata
//          Some(FunctionMd(props, domain, range))
//        }
//      } 
//      
//      case _ => None
//    }
//  }
//
//  
//  private def makeVariableFromValues(md: Metadata, elem: Elem): Option[Variable] = {
//    //check for start, increment, length definition
//    //TODO: make sure all 3 are defined
//    elem.attributes.find(_.key == "start") match {
///*
// *  
// * should cache just be array instead of IndexFunction?
// *   complications with metadata, have scalar metadata but need md for index funtion
// *   can't be the same because of "type"
// *   should IndexFunction getMd return range.md? but length is important
// */
//      case Some(att) => {
//        //TODO: don't assume doubles, look at type from md
//        val start = att.value.text.toDouble
//        val increment = (elem \ "@increment").text.toDouble
//        val length = (elem \ "@length").text.toInt //TODO: allow infinite length
//        val domain = IndexSet(length)
//        val seq = makeLinearSeq(md, start, increment, length)
//        val range = VariableSeq(seq)
//        //hack together some metedata
//        val dmd = ScalarMd(HashMap(("name", "index_"+md.name), ("type", "Integer")))
//        val fmd = FunctionMd(HashMap(("name", md.name)), dmd, md)
//        Some(Function(fmd, domain, range)) //TODO: make metadata with at least the same name/id as the scalar?
//      }
//
//      //values not defined as attributes, get from text content
//      case None => { 
//        elem.child.find(_.isInstanceOf[scala.xml.Text]) match { //TODO: better way to get content?
//          case Some(tnode) => {
//            //split values on white space
//            val ss = tnode.text.trim().split("""\s+""") //TODO: allow comma? use RegEx.DELIMITER? 
//            ss.length match {
//              case 0 => None
//              case 1 => Some(Scalar(md, ss(0)))
//              case n: Int => md match {
//                case md: ScalarMd => {
//                  val range = VariableSeq(ss.map(Scalar(md, _)))   //VariableSeq((0 until n).map(i => Scalar(md, ss(i))))
//                  //Some(IndexFunction(range)) //TODO: include scalar metadata? make new FunctionMd? but need parentage...? do we need name for cache?
//        //hack together some metedata
//                  val domain = IndexSet(n)
//        val dmd = ScalarMd(HashMap(("name", "index_"+md.name), ("type", "Integer")))
//        val fmd = FunctionMd(HashMap(("name", md.name)), dmd, md)
//        Some(Function(fmd, domain, range)) 
//                }
//                case md: TupleMd => Some(Tuple(md, ss.map(s => Real(s.toDouble)))) //assume reals, TODO: make Text if defined with ""?
//                //TODO: values for Function
//              }
//            }
//          }
//          case None => None //no values defined in text content
//        }
//      }
//    }
//  }
//  
//  
////--- Variable Construction ---------------------------------------------------
// /* TODO: ssi_flat is trying to read wavelength from currentRecord
// * look into excluding correctly      
// * permutations of excluding from model vs other source of value
// *   values defined outside function, ref in function
// *     cache and exclude from model, use ref to get it from cache instead of source
// *   in-line values within function def
// *     cache but don't exclude, replace with ref for building?
// *     no need to support this option
// *   defining data in source but not wanting to expose
// *     define all columns for parsing purposes, exclude some from model
// *     "exclude" att? PI?
// *     
// *   
// *
// */
//  
//  
//    private def makeMetadata(vml: VariableMl): Option[Metadata] = vml match {
//    //TODO: keep Map from var name to its metadata?
//    case ml: ScalarMl => 
//  }
//  
//  /**
//   * Recursively construct the data model components based on the Metadata.
//   * Note, this gets called for top level Variables: direct children
//   * of the dataset xml element. Top-level Tuples may also call it recursively.
//   * Lower level Variables (e.g. within Functions) are typically handled by subclasses'
//   * makeFunctionIterator.
//   */
//  def makeVariable(md: Metadata): Option[Variable] = {
//    //Try getting the Variable from the cache, e.g. had values defined in TSML.
//    var variable = if (refCache.contains(md.name)) refCache(md.name) 
//    else md match {
//      case md: FunctionMd => makeFunction(md)
//      case md: TupleMd => makeTuple(md) //could be Tuple or Index -> Tuple
//      case md: ScalarMd => makeScalar(md) //could be Scalar or Index -> Scalar
//    }
//    
//    variable match {
//      case v: Variable =>  md.get("exclude") match {
//        //Note, we do not exclude earlier because this may be needed to 
//        //read "junk" data from the source to get to the good stuff.
//  //TODO: use exclude processing instruction, apply as we would a projection constraint
//        case Some(s) if (s.toBoolean) => None //exclude from Dataset
//        case _ => Some(variable)
//      }
//      case _ => None //variable is null, not successfully constructed
//    }
//  }
//  
//  /**
//   * Construct a Tuple or Index -> Tuple from tuple Metadata. 
//   * If there are multiple samples, a subclass could create a Function of Index.
//   * This will typically be called only for Tuples defined outside the context
//   * of a Function.
//   */
//  protected def makeTuple(md: TupleMd): Variable = Tuple(md.elements.flatMap(makeVariable(_)))
//  
//    
//  /**
//   * Make the top-level, primary, outer Function for this Dataset by wrapping a FunctionIterator.
//   * Note, this will delegate to makeFunctionIterator to construct the Variables encapsulated
//   * within this Function. This allows us to keep our memory footprint low by constructing 
//   * Variables only as they are being requested. 
//   */
//  protected def makeFunction(metadata: FunctionMd): Function = Function(metadata, makeFunctionIteratorMaker)
//  
//  /**
//   * This abstract method should be implemented by subclasses to return a function
//   * (Scala, not LaTiS) that constructs a FunctionIterator from a Function's Metadata.
//   * This allows us to avoid accessing the data source until something invokes the
//   * function's "iterator". Construction of the FunctionIterator typically requires that
//   * the first data record be cached to simplify the hasNext contract.
//   * This might not be much better once we start doing operations on Variables.
//   * We may need to re-evaluate how to be lazy so we can manipulate the data model
//   * without accessing data.
//   */
//  protected val makeFunctionIteratorMaker: FunctionMd => FunctionIterator = null
//  //TODO: provide a default impl because this isn't applicable for all Adapters
//  
//  
//  /**
//   * Construct a Scalar or Index -> Scalar from scalar Metadata.
//   * This will typically be called only outside the context of a Function.
//   * Default to null. Some data formats have no support for Scalars outside
//   * the context of a Function.
//   */
//  protected def makeScalar(md: ScalarMd): Variable = null
//  
//  
//
//  /**
//   * Make a Seq of Reals representing monotonic values defined by
//   * start, increment, and length.
//   * TODO: could just use a Linear DomainSet, support more than Reals?
//   */
//  private def makeLinearSeq(md: Metadata, start: Double, increment: Double, _length: Int) = new Seq[Variable] {
//    //Note, use "_length" to avoid stack overflow due to Iterable's use of "length"
//    private var _index = 0
//    
//    def iterator = new Iterator[Variable] {
//      def hasNext = _index < _length
//      def next() = {
//        val r = Real(md, start + increment*_index)
//        _index += 1
//        r
//      }
//    }
//    
//    def length = _length
//    
//    def apply(index: Int) = Real(md, start + increment*index)
//    
//  }

}

object TsmlAdapter {
  
  /**
   * Construct an instance of a TsmlAdapter as defined in the TSML.
   */
  def apply(tsml: Tsml): TsmlAdapter = {
    val atts = tsml.dataset.getAdapterAttributes()
    val class_name = atts("class")
    try {
      val cls = Class.forName(class_name)
      val ctor = cls.getConstructor(tsml.getClass())
      ctor.newInstance(tsml).asInstanceOf[TsmlAdapter]
      //TODO: call init? otherwise nothing happens till getDataset, but often need ops that come with getDataset(ops)
    } catch {
      case e: Exception => {
        //e.printStackTrace()
        throw new Error("Failed to construct Adapter: " + class_name, e)
      }
    }
  }
  
}

