package latis.reader.tsml.agg

import java.io.File

import scala.collection.mutable.ArrayBuffer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Text
import latis.ops.Operation
import latis.ops.agg.Join
import latis.ops.agg.TileJoin
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.ops.filter.Selection
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.Tsml
import latis.ops.filter.NearestNeighborFilter

/**
 * A TsmlAdapter that reads data from each file in a file list 
 * and joins the results. The tsml should be structured like so: 
 * 
 * <dataset>
 *   <adapter class="latis.reader.tsml.agg.FileJoinAdapter"
 *            join="<class that implements latis.ops.agg.Join>"/>
 *   
 *   <dataset>
 *     ...
 *     This dataset returns the list of files/URLs to be read
 *     ...
 *   </dataset>
 *   
 *   <dataset>
 *     ...
 *     Defines how to read one granule (file/URL). It will be used as a template
 *     for each granule in the above dataset with the "location" updated.
 *     ...
 *   </dataset>
 *   
 * </dataset>
 *     
 */
class FileJoinAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Dynamically construct the join operation.
   */
  lazy val joinOperation: Join = {
    getProperty("join") match {
      case Some(class_name) => {
        //dynamically construct join operation
        val cls = Class.forName(class_name)
        val ctor = cls.getConstructor()
        ctor.newInstance().asInstanceOf[Join]
        //TODO: error handling
      }
      case None => TileJoin() //default
    }
  }
  
  /**
   * Construct an adapter for the file list dataset from the first "dataset"
   * definition in the tsml.
   */
  lazy val fileListAdapter = TsmlAdapter(Tsml((tsml.xml \ "dataset").head))
  //TODO: allow any reader; need to be able to ask if it has a variable by name without reading
  
  /**
   * Tsml for the template to be used to read each file.
   */
  val templateTsml = Tsml((tsml.xml \ "dataset").last)
  
  /**
   * Extract the file paths from a file list Dataset.
   */
  def getFilePaths(ds: Dataset): Iterator[String] = {
    //TODO: could get srcDir from tsml, but also needed for ZipWriter?
    lazy val dir = ds.getMetadata.get("srcDir") match { //TODO: consider "baseURL"
      case None => ""
      case Some(sd) => sd + File.separator
    }
    
    ds match {
      case Dataset(Function(it)) => it.map(_.findVariableByName("file") match { //TODO: consider "url"
        case Some(Text(file)) => dir + file
        case None => throw new Exception(s"No 'file' Variable found in Dataset '$ds'")
      })
      case _ => Iterator.empty
    }
  }
  
  /**
   * Operations to be passed to getDataset for file list.
   */
  val fileListOps = ArrayBuffer[Operation]()
  
  /**
   * Operations to be passed to getDataset for each granule Dataset.
   */
  val granuleOps = ArrayBuffer[Operation]()
  
  /**
   * Manage how operations will be applied (e.g. to file list, granules and/or 
   * joined data). Selections (including NearestNeighbor) will be offered for 
   * the template Adapter to handle for each granule. Selections not handled, 
   * though applicable (granule has the parameter name), will be applied to the
   * joined data (later) by returning "false" here. Selections will be applied 
   * to the file list dataset if applicable. First and Last filters will always
   * be applied to the file list but not to granules.
   */
  override def handleOperation(op: Operation): Boolean = op match {
    case Selection(name, _, _) => 
      if (fileListHasName(name)) fileListOps += op //apply to file list dataset
      handleGranuleOp(name, op)
    
    //Apply first and last operations to both the file list and the joined files.
    //Note the return of "false" to tell getDataset to apply them to the result.
    case ff: FirstFilter => fileListOps += ff; false
    case lf: LastFilter => fileListOps += lf; false
    
    // Apply Nearest Neighbor Operation to granules (but not file list).
    // This is not applicable to the file list.
    // Note, if the bounding pair is not found in the same granule,
    //   this will find the one closest end-point in each file
    //   greatly reducing the size of the joined dataset and minimizing 
    //   the final application without risk of losing the appropriate sample.
    case NearestNeighborFilter(name, _) => 
      var handled = handleGranuleOp(name, op)
      // If this is for the outer domain, also apply to the joined dataset
      if (outerDomainHasName(name)) handled = false
      handled
   
    case _ => false //apply any other ops to the joined data
  }

  /**
   * Does the file list dataset have a variable with the given name.
   */
  private def fileListHasName(name: String): Boolean = {
    fileListAdapter.getOrigDataset.findVariableByName(name).nonEmpty
  }
  
  /**
   * Does the domain Variable of the outer Function have the given name.
   */
  private def outerDomainHasName(name: String): Boolean = {
    val tmpAdapter = TsmlAdapter(templateTsml)
    val hasName = tmpAdapter.getOrigDataset match {
      case Dataset(f: Function) => f.getDomain.hasName(name)
      case _ => false
    }
    try { tmpAdapter.close } catch { case e: Exception => }
    hasName
  }
  
  /**
   * Operations that are targeted for a specific Variable may be applicable to
   * the granule datasets. 
   */
  private def handleGranuleOp(name: String, op: Operation): Boolean = {
    val tmpAdapter = TsmlAdapter(templateTsml)
    val granHandled = tmpAdapter.handleOperation(op)
    val granHasName = tmpAdapter.getOrigDataset.findVariableByName(name).nonEmpty
    try { tmpAdapter.close } catch { case e: Exception => }

    if (granHandled) granuleOps += op

    // If the granule has the selected variable 
    // and it is not handled by the adapter
    // then we need to apply it later to the joined data 
    // so return "false" indicating that it has not yet been applied.
    !(granHasName && !granHandled)
  }
  
  /**
   * Keep the set of readers for each file so we can close them.
   */
  private val readers = ArrayBuffer[TsmlReader]()
  
  /**
   * Make a Dataset from each file in the file list Dataset by substituting the
   * file name into the "location" of the data tsml template.
   * Use the join Operation to combine them into one Dataset.
   */
  def joinDatasets: Dataset = {
    val fileDataset = fileListAdapter.getDataset(fileListOps)
    val files = getFilePaths(fileDataset)
    //make ds from template for each file
    val dss: Iterator[Dataset] = files.flatMap { file => { 
      //make a reader with the file location plugged into the template tsml
      val reader = TsmlReader(templateTsml.dataset.setLocation(file))
      readers += reader //keep a copy so we can close later
      try {
        Some(reader.getDataset(granuleOps))
      }
      catch {
        case _: Exception => None //skip invalid files
      }
    }}
    
    //Make metadata for the joined dataset based on the tsml
    val md = makeMetadata(tsml.dataset)
    
    //Apply the join operation to the datasets
    if (dss.isEmpty) Dataset(null, md)
    else dss.reduceLeft(joinOperation(_,_)) match {
      case Dataset(v) => Dataset(v, md) //add metadata (e.g. dataset name) to the dataset
      case _ => Dataset(null, md)
    }
  }
  
  /**
   * Override to add logic to join the file datasets.
   */
  override def getDataset(ops: Seq[Operation]): Dataset = {
    //TODO: consider how PIs are handled
    //could we use the super getDataset = joinDatasets? 

    val otherOps = ops.filterNot(handleOperation(_))
    
    val ds = joinDatasets
    
    otherOps.foldLeft(ds)((dataset, op) => op(dataset)) 
  }
  
  /**
   * Close the reader for each data file.
   * Since these don't get closed until the end of a LatisServer request,
   * there is risk of having too many files open.
   */
  def close: Unit = readers.foreach(_.close)

}
