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
//import latis.time.Time


/**
 * Base class for Adapters that read dataset as defined by TSML.
 * The "dsml" constructor argument is single "dataset" child XML 
 * element of the tsml element.
 */
abstract class TsmlAdapter(val tsml: Tsml) {
  
  /**
   * Store XML attributes for this Adapter definition as a properties Map.
   */
  val properties: Map[String,String] = tsml.dataset.getAdapterAttributes()

  /*
   * TODO: 2013-06-25
   * traits for granule vs iterable?
   * but traits can't have state, e.g. data map
   *   DataGranule: column-oriented, Map, Data for each Scalar
   *   DataInterator: ByteBufferData with sample size, Data in Function
   * use same cache as used by values?
   * consider record oriented (ascii, db) vs col-oriented (netcdf, bin)
   *   
   * should parsers be traits?
   * 
   * support values in tsml
   * use dataMap idea from Granule?
   * generalize to concept of "cache"?
   * values cache is not volatile
   * 
   * 
   */
  
  
  /**
   * Hook for subclasses to apply operations during data access
   * to reduce data volumes. (e.g. query constraints to database or web service)
   * Return true if it will be handled. Otherwise, it will be applied
   * to the Dataset later.
   * The default behavior is for the Adapter to handle no operations.
   */
  def handleOperation(op: Operation): Boolean = false 
  
  
  lazy val dataset: Dataset = makeDataset()
  
  protected def makeDataset(): Dataset = {
    val md = makeMetadata(tsml.dataset)
    val vars = tsml.dataset.getVariableMl.flatMap(makeVariable(_))
    Dataset(vars, md) 
  }  
  

  
  //def getDataset(operations: Map[String, Seq[String]]): Dataset = {
  def getDataset(operations: Seq[Operation]): Dataset = {
    /*
     * TODO: handle like processing instructions
     * use Map: type => text, or Seq[String] since there can be multiple "filter"s...
     * but what about order? do we always want projections before selections...?
     *   unit conversion before filter?
     * Need to enable subclass to apply the PIs it can, removing them from the list
     * 
     * Dataset has a Seq of operations
     * some can be applied by the adapter when constructing the Dataset (before data)
     * others need to be applied when accessing Data
     *   
     */
    
    //allow subclasses to handle the ones they can
    //TODO: needs better control
    //this.operations = operations.toBuffer
    
    //Give subclass the opportunity to handle each operation.
    //They should return false if not handled thus it will be kept to be handled elsewhere.
    val ops = operations.filterNot(handleOperation(_))
    //TODO: before or after making dataset?
    //  probably before so we can apply them while making the Dataset
    //  so they'll need to be lazy
    
    val ds = dataset //TODO: make sure this is the waking up of the lazy dataset
    //Note, the original dataset will not be mutated
    
    //Apply remaining operations to the Dataset.
    ops.foldRight(ds)(_(_)) //op(ds)
    
    //TODO: too obtuse?
    //operations.filterNot(handleOperation(_)).foldRight(dataset)(_(_))
    
//    for (op <- ops) { //TODO: fold?
//      ds = op(ds)
//      println(ds)
//    }
//    ds
    
//    val ds1 = dataset
//    val ds2 = operations.foldRight(ds1)(_(_)) //operation.apply(ds)
//    ds2
  }
  
  /**
   * Create Metadata from "metadata" elements in the given Variable XML.
   */
  protected def makeMetadata(vml: VariableMl): Metadata = {
    //not recursive, each Variable's metadata is independent
    //just the XML attributes from "metadata" elements, for now
    //if name is not defined in metadata, use the tsml "id" attribute
    
    var atts = vml.getMetadataAttributes
    
    if (! atts.contains("name")) vml.getAttribute("id") match {
      case Some(name) => atts = atts + ("name" -> name)
      case None =>
    }

    Metadata(atts)
  }
  
  //-------------------------------------------------------------------------//
  
  //TODO: deal with metadata
  
  
  //this will be used only by top level Variables (or kids of top level Tuples)
  protected def makeVariable(vml: VariableMl): Option[Variable] = vml match {
    case sml: ScalarMl => makeScalar(sml)
    case tml: TupleMl  => makeTuple(tml)
    case fml: FunctionMl => makeFunction(fml)
  }
  
  protected def makeTuple(tml: TupleMl): Option[Tuple] = {
    Some(Tuple(tml.variables.flatMap(makeVariable(_))))
  }
  
  protected def makeFunction(fml: FunctionMl): Option[Function] = {
    for (domain <- makeVariable(fml.domain); range <- makeVariable(fml.range)) yield Function(domain, range)
  }
  
  protected def makeScalar(sml: ScalarMl): Option[Scalar] = {
    val md = makeMetadata(sml)
//TODO: apply projection, get name from metadata, it will have applied "id"
//  does this need to happen after renaming?
    //  depends if it is from tsml PI or later?
    
    sml.label match {
      case "real" => Some(Real(md))
      case "time" => Some(Time(md))
      
      //TODO: *** other types ***
      
      case _ => None
    }
  }
  
  //-------------------------------------------------------------------------//
  
  /**
   * Get the URL of the data source from this adapter's definition.
   * TODO: we may want to do some clever resolution of relative paths...
   */
  def getUrl(): String = {
    properties.get("url") match {
      case Some(url) => url
      case None => throw new RuntimeException("No url attribute in TSML adapter definition.")
    }
  }
  
  def close()
  
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
  def apply(tsml: Tsml) = {
    val atts = tsml.dataset.getAdapterAttributes()
    val class_name = atts("class")
    val cls = Class.forName(class_name)
    val ctor = cls.getConstructor(tsml.getClass())
    ctor.newInstance(tsml).asInstanceOf[TsmlAdapter]
  }
  
}

