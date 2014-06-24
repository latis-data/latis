package latis.util

import latis.data.Data
import latis.data.EmptyData
import latis.data.SampleData
import latis.dm.Binary
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.time.Time
import java.nio.ByteBuffer
import scala.collection.Map
import scala.collection.mutable
import latis.data.SampledData
import latis.data.seq.DataSeq
import scala.collection.mutable.ArrayBuffer

/**
 * Utility methods for manipulating data.
 */
object DataUtils {
  
  /**
   * Convert the Data representing variableTemplate1 to Data representing variableTemplate2.
   */
  def reshapeData(data: Data, variableTemplate1: Variable, variableTemplate2: Variable): Data = {
    val dataMap = dataToDataMap(data, variableTemplate1)
    makeDataFromDataMap(dataMap, variableTemplate2)
  }
    
  /**
   * Convert the SampleData representing sampleTemplate1 to SampleData representing sampleTemplate2.
   */
  def reshapeSampleData(sampleData: SampleData, sampleTemplate1: Sample, sampleTemplate2: Sample): SampleData = {
    val dataMap = dataToDataMap(sampleData, sampleTemplate1)
    dataMapToSampleData(dataMap, sampleTemplate2)
  }
  
  /**
   * Convert Data for a given variableTemplate to a Map from Variable name to its Data.
   */
  def dataToDataMap(data: Data, variableTemplate: Variable): Map[String, Data] = {
    buildMapFromBuffer(data.getByteBuffer, mutable.Map[String,Data](), variableTemplate)
  }

  /**
   * Recursively build Data Map from a ByteBuffer representing the given variableTemplate.
   */
  private def buildMapFromBuffer(bb: ByteBuffer, dataMap: mutable.Map[String,Data], variableTemplate: Variable): Map[String,Data] = variableTemplate match {
    case s: Scalar => {
      val bytes = new Array[Byte](s.getSize)
      bb.get(bytes)
      val data = Data(bytes)
      val name = s.getName
      dataMap.get(name) match {
        case Some(d) => dataMap += (name -> (d concat data))
        case None    => dataMap += (name -> data)
      }
    }
    
    //assume Tuple does not contain data
    case Tuple(vars) => vars.foreach(buildMapFromBuffer(bb, dataMap, _)); dataMap
    
    //apply to each sample
    case Function(it) => it.foreach(buildMapFromBuffer(bb, dataMap, _)); dataMap
  }
  
  /**
   * Convert a Data Map to SampledData representing the given sampleTemplate.
   */
  def dataMapToSampleData(dataMap: Map[String, Data], sampleTemplate: Sample): SampleData = {
    val domainData = makeDataFromDataMap(dataMap, sampleTemplate.domain)
    val rangeData = makeDataFromDataMap(dataMap, sampleTemplate.range)
    SampleData(domainData, rangeData)
  }
  
    
  /**
   * Given a data map from variable name to DataSeq (e.g. TsmlAdapter cache) and a Sample template,
   * construct SampledData that can be used when constructing a Sampled Function.
   */
  def dataMapToSampledData(dataMap: Map[String,DataSeq], sampleTemplate: Sample): SampledData = {
    /*
     * TODO: generalize for more complex data
     * +IndexSet
     */
    val vars = sampleTemplate.toSeq
    val n = dataMap(vars(0).getName).length
    //TODO: zip with index...?
    val data = ArrayBuffer[SampleData]()
    for (i <- 0 until n) {
      val f = (v: Variable) => (v.getName, dataMap(v.getName)(i))
      val m: Map[String,Data] = vars.foldLeft(Map[String,Data]())(_ + f(_))
      val sdata = DataUtils.dataMapToSampleData(m, sampleTemplate)
      data += sdata
    }
    
    SampledData(data.iterator, sampleTemplate)
  }
    
  /**
   * Given a dataMap mapping Variable names to Data and a Variable template,
   * construct a Data object with data from the dataMap.
   */
  def makeDataFromDataMap(dataMap: Map[String, Data], variableTemplate: Variable): Data = {
    //TODO: dataMapToData?
    
    //build a ByteBuffer to contain the data
    val size = variableTemplate.getSize
    val bb = ByteBuffer.allocate(size)

    //Internal method to recursively populate the buffer
    def accumulateData(v: Variable) {
      //See if we have cached data for the given variable.
      //If not, keep iterating recursively.
      dataMap.get(v.getName) match {
        case Some(d) => {
          //TODO: consider trace debugging here
          val dbb = d.getByteBuffer
          val bytes = dbb.array
          bb.put(bytes)
        }
        case None => v match {
          //case _: Index => //bb.putInt(index)  //handle Index which should not have a value in the dataMap
          case Tuple(vars) => vars.map(accumulateData(_))
          case Function(it) => it.map(accumulateData(_))
          case _: Scalar => throw new Error("No data found for " + v.getName)
        }
      }
    }

    //recursively populate the byte buffer
    accumulateData(variableTemplate)

    //TODO: test that we got the size right
    Data(bb)
  }

  /**
   * Convert a Sample (with data) to SampledData.
   */
  def sampleToData(sample: Sample): SampleData = {
    val ddata = buildDataFromVariable(sample.domain)
    val rdata = buildDataFromVariable(sample.range)
    SampleData(ddata, rdata)
  }

  /**
   * Recursively accumulate Data (bytes) for a given Variable (that contains Data).
   */
  private def buildDataFromVariable(variable: Variable, data: Data = EmptyData): Data = variable match {
    case s: Scalar => data concat s.getData
    case Tuple(vars) => vars.foldLeft(data)(_ concat _.getData)
    case Function(it) => it.foldLeft(data)(_ concat _.getData)
  }
  
  
  /**
   * Construct a Sample from the template with the Data.
   */
  def dataToSample(data: Data, template: Sample): Sample = {
//    data match {
//TODO: could use Tuple with Data
//    case sd: SampleData => 

    val bb = data.getByteBuffer
    val domain = buildVarFromBuffer(bb, template.domain)
    val range = buildVarFromBuffer(bb, template.range)
    bb.rewind //reset to the beginning in case we want to reuse it
    Sample(domain, range)
  }

  /**
   * Construct a Variable from the template with the Data.
   */
  def dataToVariable(data: Data, template: Variable): Variable = template match {
    case tup: Tuple => {
      //don't allow tuple to contain its own data, for now
      val bb = data.getByteBuffer
      val v = buildVarFromBuffer(bb, template)
      bb.rewind //reset to the beginning in case we want to reuse it
      v
    }
    case t: Time    => t(data) //Time(template.getMetadata, data)
    case _: Real    => Real(template.getMetadata, data)
    case _: Integer => Integer(template.getMetadata, data)
    case _: Text    => Text(template.getMetadata, data)
    case _: Binary  => Binary(template.getMetadata, data)
    
    //deal with nested Function
    case f: Function => data match {
      case sd: SampledData => Function(f, sd) //data already structured as SampledData
      case _ => buildVarFromBuffer(data.getByteBuffer, f) //stitch it together from bytes
    }
  }

  /**
   * Recursively construct Variables with the given template and ByteBuffer data.
   */
  def buildVarFromBuffer(bb: ByteBuffer, template: Variable): Variable = template match {
    case v: Time => v match {
      case _: Real => Time(template.getMetadata, bb.getDouble)
      case _: Integer => Time(template.getMetadata, bb.getLong)
      case t: Text => {
        val sb = new StringBuilder
        for (i <- 0 until t.length) sb append bb.getChar
        //Time(template.getMetadata, sb.toString)
        template(Data(sb.toString))
      }
    }
    
    case v: Index => Index(bb.getInt)
    case v: Real => Real(template.getMetadata, bb.getDouble)
    case v: Integer => Integer(template.getMetadata, bb.getLong)
    
    case v: Text => {
      val cs = new Array[Char](v.length)
      bb.asCharBuffer.get(cs)
      bb.position(bb.position + v.length * 2) //advance position in underlying buffer
      //val s = (0 until v.length).map(bb.getChar).mkString
      //TODO: why can't we just get chars from the bb?
      val s = new String(cs)
      Text(template.getMetadata, s)
    }
    
    case v: Binary => {
      val bytes = new Array[Byte](v.getSize)
      bb.get(bytes)
      val buffer = ByteBuffer.wrap(bytes)
      Binary(template.getMetadata, buffer)
    }
      
    case Tuple(vars) => Tuple(vars.map(buildVarFromBuffer(bb, _)), template.getMetadata)

    /*
     * deal with nested Function
     * TODO: just put data in new Function as SampledData?
     * just iterate through the whole thing, for now
     */
    case f: Function => {
      val n = f.getLength
      val smp = Sample(f.getDomain, f.getRange)
      if (n < 0) throw new Error("Function length not defined") //TODO: consider "-n" as unlimited but currently has n
      //TODO: warn if 0?
      else {
        val samples = (0 until n).map(i => buildVarFromBuffer(bb, smp))
        Function(samples, f.getMetadata)
      }

    }
  }
}