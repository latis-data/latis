package latis.reader.tsml

import scala.collection.mutable.ArrayBuffer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.ops.DomainBinner
import latis.ops.Operation
import latis.ops.filter.Selection
import latis.reader.tsml.ml.Tsml
import latis.reader.tsml.ml.TsmlResolver
import latis.util.iterator.PeekIterator

class FileAggAdapter(tsml: Tsml) extends FileListAdapter(tsml) {
  
  /**
   * Ops which will be applied when reading the file list.
   */
  var fileOps = {
    //get the DomainBinner proccessing instruction if available
    val domBin = tsml.getProcessingInstructions("domBin")
    ArrayBuffer[Operation](domBin.map(s => DomainBinner(s.split(","))):_*)
  }
  /**
   * Ops which will be applied when reading each individual file.
   */
  var aggOps = ArrayBuffer[Operation]()
  
  /**
   * A tsml that describes a dataset of the desired structure for this dataset.
   */
  lazy val template = getProperty("template") match {
    //the template attribute should be the name of the template dataset
    case Some(name) => TsmlResolver.fromName(name)
    case None => throw new Exception("Nrl2AggAdapter requires 'template' attribute in tsml")
  }
  
  /**
   * A name of a dataset that will give the wanted file list. If not specified, 
   * the file list will be read from the tsml of this adapter using FileListAdapter.
   */
  lazy val fileList = getProperty("fileList") match {
    case Some(name) => TsmlReader(TsmlResolver.fromName(name))
    case None => null
  }
      
  /**
   * The name of each file to be read. Can be filtered with fileOps.
   */
  lazy val getFileList = {
    val ds = fileList match {
      case null => new FileListAdapter(tsml).getDataset(fileOps)
      case _ => fileList.getDataset(fileOps)
    }
    
    ds match {
      case Dataset(f: Function) => f.iterator.map(_.toSeq.find(_.hasName("file")).get.getValue.toString)
    }
  }
  
  /**
   * Read a dataset from each file in 'files' and combine the Sample iterator from each dataset.
   */
  def filesToSamplesIterator(files: Iterator[String]): PeekIterator[Sample] = {
    PeekIterator(files.map(filename =>  TsmlReader(template.setLocation(filename)))
      .flatMap(adapter => adapter.getDataset(aggOps) match {
        case Dataset(Function(it)) => it
      }))
  }
  
  /**
   * Pass time selection to the FileListAdapter. 
   */
  override def handleOperation(op: Operation): Boolean = op match {
    case Selection("time", o, value) => o match {
      case "<" | "<=" => {
        aggOps += op
        fileOps += Selection("start_time", o, value)
        false
      }
      case ">" | ">=" => {
        aggOps += op
        fileOps += Selection("end_time", o, value)
        false
      }
    }
    case _ => {
      aggOps += op
      super.handleOperation(op)
    }
  }    
  
  override def getDataset = {
    val dir = getUrl.toString.replaceAll(" ", "%20")
    val files = getFileList.map(dir + "/" + _)
    val it = filesToSamplesIterator(files)
    val stemp = it.peek
    Dataset(Function(stemp.domain, stemp.range, it), getOrigDataset.getMetadata)
  }
    
}
