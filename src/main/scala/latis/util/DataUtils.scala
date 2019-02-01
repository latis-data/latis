package latis.util

import java.nio.ByteBuffer

import scala.collection.Map
import scala.collection.mutable

import latis.data.Data
import latis.data.EmptyData
import latis.data.IterableData
import latis.data.SampleData
import latis.data.SampledData
import latis.data.seq.DataSeq
import latis.data.set.IndexSet
import latis.data.value.StringValue
import latis.dm.Binary
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Number
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.time.Time

/*
 * Use Cases
 * 
 * VariableTemplate + Data => Variable with Data
 *   recurse to put data into kids? probably, otherwise don't need util?
 *   assume no data in template?
 * VariableTemplate + ByteBuffer => Variable with Data
 *   needed? just getBuffer from Data? but Data could be structured, SeqData...
 * 
 * Routines:
 *   dataToDataMap(data, vtmp): Map
 *     data to buffer then take byes as we build
 *   
 *   dataMapToSampleData(Map, sampleTemplate: Sample): SampleData 
 *   dataMapToSampledData(dataMap: Map[String, DataSeq], sampleTemplate: Sample): SampledData
 *   makeDataFromDataMap //TODO: dataMapToData?
 *   
 *   dataToVariable
 *   dataToSample
 *   buildVarFromBuffer(bytes, vtemp): V  //TODO: bufferToVar?
 *   
 *   sampleToData(sample: Sample): SampleData
 *   
 *   reshapeData(data, vtmp1, vtmp2)
 *     dataToMap with vtmp1
 *     map to data with vtmp2
 *   reshapeSampleData
 *     allows us to do one sample at a time, e.g. Operation with MappingIterator
 */

/**
 * Utility methods for manipulating data.
 */
object DataUtils {
  
  /*
   * TODO: want Data to be immutable but often need to recursive access ByteBuffer which changes its position.
   * should data.getByteBuffer always make duplicate? same data, diff pointer...
   * 
   */

  /**
   * Convert the Data representing variableTemplate1 to Data representing variableTemplate2.
   */
  def reshapeData(data: Data, variableTemplate1: Variable, variableTemplate2: Variable): Data = {
    //TODO: won't work for nested Functions, use reshapeSampleData?
    //val dataMap = dataToDataMap(data, variableTemplate1)
    //makeDataFromDataMap(dataMap, variableTemplate2)
    val bufferMap = dataToBufferMap(data, variableTemplate1)
    bufferMapToData(bufferMap, variableTemplate2)
 /*
  * variableTemplate2 is (a,b) instead of (i,(a,b))
  */
  }

  /**
   * Convert the SampleData representing sampleTemplate1 to SampleData representing sampleTemplate2.
   */
  def reshapeSampleData(sampleData: SampleData, sampleTemplate1: Sample, sampleTemplate2: Sample): SampleData = {
    val bufferMap = dataToBufferMap(sampleData, sampleTemplate1) 
 //*** range of sampleData is empty!? 1st outer sample, after writing, no projection yet/
    bufferMapToSampleData(bufferMap, sampleTemplate2)
  }

  private def writeBufferAsDoubles(bb: ByteBuffer) = {
    //val bb = getByteBuffer.rewind.asInstanceOf[ByteBuffer]
    val db = bb.rewind.asInstanceOf[ByteBuffer].asDoubleBuffer
    val n = db.limit()
    for (i <- 0 until n) println(db.get(i))
  }
  
  /**
   * Convert Data for a given variableTemplate to a Map from Variable name to its Data.
   */
  private def dataToDataMap(data: Data, variableTemplate: Variable): Map[String, Data] = {
    val dataMap = mutable.Map[String, Data]()
    buildMapFromBuffer(data.getByteBuffer, dataMap, variableTemplate)
    dataMap
  }

  private def dataToBufferMap(data: Data, variableTemplate: Variable): Map[String, ByteBuffer] = {
    val bufferMap = mutable.Map[String, ByteBuffer]()
    buildBufferMapFromBuffer(data.getByteBuffer, bufferMap, variableTemplate)
    //TODO: don't include index value when getting bytes from data?
    bufferMap
  }
    
  //take from one, give to other organized by var name
  private def buildBufferMapFromBuffer(bb: ByteBuffer, bufferMap: mutable.Map[String, ByteBuffer], variableTemplate: Variable): Unit = variableTemplate match {
    //TODO: avoid putting index value in Data? 
    //case _: Index => ???  //Note, from Tuple match below, let scalar handle it
    case s: Scalar => {
      //get the bytes for this scalar
      val bytes = new Array[Byte](s.getSize)
      bb.get(bytes)
      
      //add bytes to the ByteBuffer mapped by variable name
      val name = s.getName
      bufferMap.get(name) match {
        case Some(buffer) => bufferMap += (name -> ByteBuffer.wrap(buffer.array ++ bytes))
        case None => bufferMap += (name -> ByteBuffer.wrap(bytes))
      }
    }
    case Tuple(vars) => vars.foreach(buildBufferMapFromBuffer(bb, bufferMap, _))
    case f: Function => {
      val n = f.getLength
      val sampleTemplate = Sample(f.getDomain, f.getRange)  //TODO: bb 8,8, (w,(a, b))
      for (_ <- 0 until n) buildBufferMapFromBuffer(bb, bufferMap, sampleTemplate)
    }
  }
  
  /**
   * Recursively build Data Map from a ByteBuffer representing the given variableTemplate.
   */
  private def buildMapFromBuffer(bb: ByteBuffer, dataMap: mutable.Map[String, Data], variableTemplate: Variable): Unit = variableTemplate match {
    case _: Index => ???  
    case s: Scalar => {
      val bytes = new Array[Byte](s.getSize)
      bb.get(bytes)
      val data = Data(bytes)
      val name = s.getName
      dataMap.get(name) match {
        case Some(d) => dataMap += (name -> (d concat data))
        case None => dataMap += (name -> data)
      }
    }

    //assume Tuple does not contain data
    case Tuple(vars) => vars.foreach(buildMapFromBuffer(bb, dataMap, _))

    //apply to each sample
    //Note, this Function is just a template, can't iterate over samples.
    case f: Function => {
      val n = f.getLength
      val sampleTemplate = Sample(f.getDomain, f.getRange)
      for (i <- 0 until n) buildMapFromBuffer(bb, dataMap, sampleTemplate)
    }
  }

  /**
   * Convert a Data Map to SampleData representing the given sampleTemplate.
   */
  def dataMapToSampleData(dataMap: Map[String, Data], sampleTemplate: Sample): SampleData = {
    val domainData = makeDataFromDataMap(dataMap, sampleTemplate.domain)
    val rangeData = makeDataFromDataMap(dataMap, sampleTemplate.range)
    SampleData(domainData, rangeData)
  }  
  
  def bufferMapToSampleData(bufferMap: Map[String, ByteBuffer], sampleTemplate: Sample): SampleData = {
    val domainData = bufferMapToData(bufferMap, sampleTemplate.domain)
    val rangeData = bufferMapToData(bufferMap, sampleTemplate.range)
    SampleData(domainData, rangeData)
  }
  
  /**
   * Given a data map from variable name to DataSeq (column oriented, e.g. TsmlAdapter cache) and a Sample template,
   * construct SampledData that can be used when constructing a Sampled Function.
   */
  def dataMapToSampledData(dataMap: Map[String, DataSeq], sampleTemplate: Sample): SampledData = {
    //TODO: consider nested function without consistent domain samples (non-cartesian)
    //TODO: consider laziness, always wrap iterator? IterableOnce issues, Stream?

    /**
     * Internal helper function to make IterableData from map of Data Iterators so we can recursively pull off samples as we need.
     */
    def iteratorMapToIterableData(iteratorMap: Map[String, Iterator[Data]], variableTemplate: Variable, length: Int): IterableData = variableTemplate match {
      case _: Index => ???
      case s: Scalar => {
        val it = iteratorMap(s.getName)  // IndexedSeqLike$Elements, same object each time but doesn't act like iterator!?
        val datas = it.take(length).toList
        DataSeq(datas)
        //DataSeq(iteratorMap(s.getName).take(length).toSeq)
      }
      case Tuple(vars) => {
        val datass: Seq[IterableData] = vars.map(iteratorMapToIterableData(iteratorMap, _, length))
        //interleave tuple elements
        datass.tail.fold(datass.head)(_.toSeq zip _.toSeq) //TODO: cleaner way? add util method
      }
      case f: Function => { //just a Function template
        //TODO: support arbitrary nested Function domains (non-cartesian)
        //use dataMap to reuse same inner domain values for each outer sample
        val domainData = f.getDomain match {
          case i: Index => IndexSet(f.getLength)
          case e => dataMap(e.getName)
        }
        
        //for each outer sample, construct the SampledData for this nested Function
        val datas = (0 until length).map{ i =>
          val rangeData = iteratorMapToIterableData(iteratorMap, f.getRange, domainData.length);
          //val rangeData = DataSeq(rdatas)
          SampledData(domainData, rangeData)
        }
        
        //combine the 'length' Function Data-s into a single IterableData
        DataSeq(datas)
      }
    }

    //TODO: clean up duplication below with inner function above
    
    //turn data map into map of iterators so we can pull off samples as we build this
    //val iteratorMap = dataMap.map(kv => (kv._1, kv._2.iterator))
    val iteratorMap = dataMap.map{ kv => 
      val name = kv._1
      val datas = kv._2
      (name, datas.iterator)
    }
    
    //Makes data from the cartesian product of mulitple data seq's. 
    //Used for combining the data of nD domains. 
    def productSet(s1: Seq[Data], ss: Seq[Data]*): Seq[Data] = ss.length match {
      case 0 => s1
      case 1 => for(d1 <- s1; d2 <- ss.head) yield d1 concat d2
      case _ => productSet(productSet(s1, ss.head), ss.tail:_*)
    }
    
    val domain = sampleTemplate.domain
    val domainData: IterableData = domain match {
      case _: Index => IndexSet()
      case s: Scalar => dataMap(s.getName)
      case t: Tuple => {
        val datas = t.toSeq.map(s => dataMap(s.getName).iterator.toSeq)
        IterableData(productSet(datas.head, datas.tail: _*))
        //what if we are given data which already represents the product?
      }
    }

    val range = sampleTemplate.range

    val length = domainData.length //number of dimensions?

    val rangeData = iteratorMapToIterableData(iteratorMap, range, length)

    SampledData(domainData, rangeData)

    //    val vars = sampleTemplate.toSeq
    //    val n = dataMap(vars(0).getName).length
    //    //TODO: zip with index...?
    //    val data = ArrayBuffer[SampleData]()
    //    for (i <- 0 until n) {
    //      val f = (v: Variable) => (v.getName, dataMap(v.getName)(i))
    //      val m: Map[String,Data] = vars.foldLeft(Map[String,Data]())(_ + f(_))
    //      val sdata = DataUtils.dataMapToSampleData(m, sampleTemplate)
    //      data += sdata
    //    }
    //    
    //    SampledData(data.iterator, sampleTemplate)
  }

  private def bufferMapToData(dataMap: Map[String, ByteBuffer], variableTemplate: Variable): Data = {
    //build a ByteBuffer to contain the data
    val size = variableTemplate.getSize
    val bb = ByteBuffer.allocate(size)
    
    //Internal method to recursively populate the buffer
    def accumulateData(v: Variable) {
      //See if we have cached data for the given variable.
      //If not, keep iterating recursively.
      //Assumes only scalars are mapped to data
      dataMap.get(v.getName) match {
        case Some(buffer) => {
          val bytes = new Array[Byte](v.getSize)
          buffer.get(bytes)
          bb.put(bytes)
        }
        case None => v match {
/*
 * TODO: should Data contain Index values? hopefully we can just use IndexSet
 * problem for TestProjection.project_all_but_inner_domain_in_function_function
 * but getting BufferUnderflow before here (after writing starts)
 * 
 */     
          case _: Index => {
            ???
          }
          case Tuple(vars) => vars.map(accumulateData(_))
          case f: Function => {
            //TODO: recurring pattern; apply sample template f.length times
            val sample = Sample(f.getDomain, f.getRange)
            val n = f.getLength
            for (i <- 0 until n) accumulateData(sample)
          }
          case _: Scalar => {
            throw new RuntimeException("No data found for " + v.getName)
          }
        }
      }
    }
    
    //recursively populate the byte buffer
    accumulateData(variableTemplate)
    Data(bb)
  }
  
  /**
   * Given a dataMap mapping Variable names to Data and a Variable template,
   * construct a Data object with data from the dataMap.
   */
  def makeDataFromDataMap(dataMap: Map[String, Data], variableTemplate: Variable): Data = {
    //Used here and by IterativeAdapter.
    //TODO: dataMapToData?

    //build a ByteBuffer to contain the data
    val size = variableTemplate.getSize
    val bb = ByteBuffer.allocate(size)

    //Internal method to recursively populate the buffer
    def accumulateData(v: Variable): Unit = {
      //See if we have cached data for the given variable.
      //If not, keep iterating recursively.
      dataMap.get(v.getName) match {
        case Some(d) => {
          //TODO: consider trace debugging here
          /*
           * TODO: broken for nested Functions, use bufferMap
           * dataMap gives us all samples for this scalar
           * we need to get one but don't have the means to sub-select
           */
          val dbb = d.getByteBuffer
          val bytes = dbb.array
          bb.put(bytes)
        }
        case None => v match {
          case Tuple(vars) => vars.foreach(accumulateData(_))

          //can't iterate on template case Function(it) => it.map(accumulateData(_)) 
          case f: Function => {
            //TODO: recurring pattern; apply sample template f.length times
            val sample = Sample(f.getDomain, f.getRange)
            val n = f.getLength
            for (i <- 0 until n) {
              accumulateData(sample)
            }
          }

          case _: Index => ??? //bb.putInt(index)  //handle Index which should not have a value in the dataMap
          case _: Scalar => {
            throw new RuntimeException("No data found for " + v.getName)
          }
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
  def buildDataFromVariable(variable: Variable, data: Data = EmptyData): Data = variable match {
    case i: Index => data concat i.getData
    case t @ Text(s) => data concat StringValue(StringUtils.padOrTruncate(s, t.length)) //need consistent length for binary
    case s: Scalar => data concat s.getData
    case Tuple(vars) => vars.foldLeft(data)((d,v) => buildDataFromVariable(v,d))
    case f @ Function(it) => it.foldLeft(data)((d,v) => buildDataFromVariable(v,d))
  }

  /**
   * Construct a Sample from the template with the Data.
   */
  def dataToSample(data: Data, template: Sample): Sample = {
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
    case t: Time => t(data) //Time(template.getMetadata, data)
    case _: Real => Real(template.getMetadata, data)
    case _: Integer => Integer(template.getMetadata, data)
    case _: Text => Text(template.getMetadata, data)
    case _: Binary => Binary(template.getMetadata, data)
    case _: Index => ???

    //deal with nested Function
    case f: Function => data match {
      case sd: SampledData => Function(f, sd) //data already structured as SampledData
      case _ => buildVarFromBuffer(data.getByteBuffer, f) //stitch it together from bytes
    }
  }
  
  /**
   * Convert the first nchar * 2 bytes from the given ByteBuffer into a String
   * of nchar characters. The factor of 2 is needed because a char is 2 bytes in Java.
   */
  def bufferToString(bb: ByteBuffer, nchar: Int): String = {
    val cs = new Array[Char](nchar)      //allocate an array of chars just big enough for our String
    bb.asCharBuffer.get(cs)              //load the array (cs) with nchar*2 bytes
    bb.position(bb.position() + nchar * 2) //advance position in underlying buffer
    new String(cs)                       //make a String out of our array
  }

  /**
   * Recursively construct Variables with the given template and ByteBuffer data.
   */
  def buildVarFromBuffer(bb: ByteBuffer, template: Variable): Variable = template match {
    case v: Time => v match {
      case _: Real => Time(template.getMetadata, bb.getDouble)
      case _: Integer => Time(template.getMetadata, bb.getLong)
      case t: Text => {
        val n = t.length
        val s = bufferToString(bb, n)
        v(Data(s)) //make copy of this Time variable but with new Data
      }
    }

    case v: Index => Index(v.getMetadata, bb.getInt) //Note, index data is kept in Variable's Data
    case v: Real => Real(template.getMetadata, bb.getDouble)
    case v: Integer => Integer(template.getMetadata, bb.getLong)

    case v: Text => {
      val n = v.length
      val s = bufferToString(bb, n)
      Text(template.getMetadata, s)
    }

    case v: Binary => {
      val bytes = new Array[Byte](v.getSize)
      bb.get(bytes)
      val buffer = ByteBuffer.wrap(bytes)
      Binary(template.getMetadata, buffer)
    }

    //make sure Samples remain Samples
    case Sample(domain, range) => Sample(buildVarFromBuffer(bb, domain), buildVarFromBuffer(bb, range))

    case Tuple(vars) => Tuple(vars.map(buildVarFromBuffer(bb, _)), template.getMetadata)

    /*
     * deal with nested Function
     * TODO: just put data in new Function as SampledData?
     * just iterate through the whole thing, for now
     * TODO: does this only get called for nested Functions?
     */
    case f: Function => {
      //Require that length of nested Function be specified in metadata. It likely doesn't contain its own data so can't iterate to get length.
      val n = f.getMetadata("length") match {
        case Some(s) => s.toInt
        case None => throw new RuntimeException("Nested Function must have 'length' defined.")
      }

      val smp = Sample(f.getDomain, f.getRange)
      if (n < 0) throw new RuntimeException("Function length not defined") //TODO: consider "-n" as unlimited but currently has n
      //TODO: warn if 0?
      else {
        val samples = (0 until n).map(i => buildVarFromBuffer(bb, smp))
        Function(samples, f.getMetadata)
      }

    }
  }
  
  /**
   * Return the data value of the given Variable as a Double.
   * This applies only to Numeric or Time Scalars. Any others with return NaN.
   * If the Variable is Time of type Text, the value will be in milliseconds since 1970-01-01.
   */
  def getDoubleValue(variable: Variable): Double = variable match {
    case Number(d) => d
    //TODO: deal with empty data
    case _ => Double.NaN
  }
  
  /**
   * Return the given String value as a Double interpreting it as a data value
   * from the given template variable. 
   * This applies only to Numeric or Time Scalars. Any others with return NaN.
   * This handles the case of different Time types:
   *   If Time is Text, the value is interpreted as an ISO 8601 string.
   *   If Time is Real or Integer and the value is NOT convertable to a double, 
   *     it is assumed to be an ISO string and will be converted to the native units of the Time template.
   */
  def parseDoubleValue(template: Variable, value: String): Double = template match {
    case t: Time => t match {
      case _: Text => Time.isoToJava(value).toDouble  //if variable is a Text Time, assume value is ISO format
      case _: Number => {
        if (StringUtils.isNumeric(value)) value.toDouble  //if value is a number assume native units
        else {  //assume value is an iso string
          val timeScale = t.getUnits  //TODO: error if no units? assume default?
          val t2 = Time.fromIso(value).convert(timeScale)
          DataUtils.getDoubleValue(t2)
        }
      }
    }
    case _: Number => value.toDouble
    case _ => Double.NaN
  }
  
  /**
   * Since Binary Variables can have variable lengths but we currently expect
   * fixed length Data (akin to Text), we need to be able to mark the 
   * end of the useful bytes and drop the padding. We can't simply use 0b since
   * it may occur in valid data. Instead, we use a special sequence of 8 bytes
   * to serve as a marker.
   * If the marker does not exist, return the entire array.
   */
  def trimBytes(bytes: Array[Byte]): Array[Byte] = {
    bytes.indexOfSlice(nullMark) match {
      case -1 => bytes
      case index: Int => bytes.take(index)
    }
  }
  
  /**
   * Sequence of 8 bytes to use to end a section of useful bytes in a byte array.
   */
  val nullMark: Array[Byte] = "nullMark".getBytes
  //110, 117, 108, 108, 77, 97, 114, 107
  
  /**
   * Make sure the bytes contained in the given ByteBuffer have a termination mark.
   * The mark will be added after the 'limit' position. The size of the array might
   * grow to accommodate the 8 byte mark, so don't do this to data that is already
   * part of a Binary variable without updating the 'length' metadata.
   */
  def terminateBytes(buffer: ByteBuffer): ByteBuffer = {
    val limit = buffer.limit()
    val capacity = buffer.capacity

    val bytes = buffer.array //actual backing array, mutable
    
    if (bytes.lastIndexOfSlice(nullMark) >= 0) {
      //already has mark, no need to do anything
      buffer
    } else {
      //need to add mark
      if (capacity - limit < 8) {
        //need to increase size of array to accommodate 8 byte mark
        val bb = ByteBuffer.allocate(limit + 8)
        bb.put(bytes, 0, limit) //add original valid bytes
        bb.put(nullMark) //add termination mark
        bb.flip.asInstanceOf[ByteBuffer]  //set limit and rewind
      } else {
        //we have enough room to add the mark
        buffer.position(limit) //set the position to the end of the valid data
        buffer.put(nullMark) //add termination mark
        buffer.flip.asInstanceOf[ByteBuffer]  //set limit and rewind
      }
    } 
  }
  
}
