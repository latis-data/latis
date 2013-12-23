package latis.reader.tsml

import latis.data._
import latis.dm._
import java.sql.{Connection, DriverManager, ResultSet}
import java.nio.ByteBuffer
import javax.naming.Context
import javax.naming.InitialContext
import javax.sql.DataSource
import latis.util.NextIterator
import latis.time.Time
import java.util.Calendar
import java.util.TimeZone
import latis.ops._
import scala.collection.mutable.ArrayBuffer
import java.sql.Statement
import latis.util.RegEx._
import latis.util.RegEx
import latis.time.TimeScale
import latis.reader.tsml.ml.ScalarMl
import latis.reader.tsml.ml.Tsml
import com.typesafe.scalalogging.slf4j.Logging

class JdbcAdapter(tsml: Tsml) extends IterativeAdapter(tsml) with Logging {
  
  //TODO: catch exceptions and close connections
    
  //Handle the Projection and Selection Operation-s
  private var projection = "*"
  private val selections = ArrayBuffer[String]()
  
  
  //Handle first, last ops
  private var first = false
  private var last = false
  
  //Define sorting order.
  private var order = " ASC"
    
  override def getDataset(ops: Seq[Operation]): Dataset = {
    //val ds = dataset
    /*
     * TODO 2013-10-31
     * but construction of the dataset applies the handled ops
     * but all lazily until writer starts asking for data
     * 
     * projection broken:
     * probably because dataset is mode before proj op is applied
     * we need to deliver the final dataset, not the orig
     * but we don't want to use default Projection Op
     * ++handling ops needs to munge dataset model (no data until iterator is invoked)
     * but tsml can have values = data
     * 
     * should just delegate to proper ops even if they are redundant
     * just use what we can to make source data access efficient
     * 
     * does makeFunction,makeDataIterator need to run after projection applied?
     * default projection op should just wrap it
     * but proj op will expect data to be consistent with the model when it removes a var...
     * handleOp could apply op and return new dataset?
     * they key is to end up with a new dataset after each op
     * safe to assume here (no values in tsml) that there's no data in dataset at this stage?
     * 
     */
    
    val others = (piOps ++ ops).filterNot(handleOperation(_))
    //ops.map(handleOperation(_))
    
    val ds = dataset
    
    //TODO: call super to handle the rest?
    //TODO: or applyOps(ds, ops)?
   
    others.reverse.foldRight(ds)(_(_))
  }
  
  override def handleOperation(operation: Operation): Boolean = operation match {
    case Projection(p) => {this.projection = p; true}
    //TODO: support alias
    //getVariableByName(alias).getName
    //do we really want to impl that for Tsml? in addition to Dataset?
    
    /*
     * TODO: need to keep orig dataset, filter on non-projected vars...
     * note, we do not yet have a dataset.
     * We currently ask adapters whether they can and will handle the ops when the dataset is requested.
     * 
     * Is there any reason not to create an orig dataset first?
     * Simply reflects the tsml, no data access needed.
     * Seems like a better idea.
     * operation handlers could still be lazy
     * consider how we use "template" variables
     * 
     */
    
    case Selection(expression) => expression match {
      //Break expression up into components
      //TODO: sanitize value, quotes around timetag,...
      case SELECTION(name, op, value) => {
        if (name == "time") handleTimeSelection(op, value) //special handling for "time"
        else if (tsml.getScalarNames.contains(name)) { //other variable (not time), even if not projected
          //add a selection to the sql, may need to change operation
          op match {
            case "==" => selections append name + "=" + value; true
            case "=~" => selections append name + " like '%" + value + "%'"; true
            case "~"  => false  //almost equal (e.g. nearest sample) not supported by sql
            case _ => selections append expression; true
          }
        }
        else false //doesn't apply to our variables, so leave it for the next handler, TODO: or error?
      }
      //TODO: case _ => doesn't match selection regex, error
    }
    
    case _: FirstFilter => first = true; true
    case _: LastFilter  => last = true; order = " DESC"; true
    
    //TODO: handle exception, return false (not handled)?
    
    case _ => false //not an operation that we can handle
  }

 /*
  * TODO: time format for sql
  * dataset.findTimeVariable? 
  * numeric units: if query string matched TIME regex, convert iso to var's timeScale
  * datetime: what should tsml units be?
  *   will be converted to java time
  *   but we need to know if the db uses datetime so we can form sql
  *   units="ISO"?
  *   use "format", save units for numeric times, default to java
  * Support Time as real, integer or string
  *   for the db datetime case, use string
  *   until then, use "format"
  */
  def handleTimeSelection(op: String, value: String): Boolean = {
    //support ISO time string as value
    //TODO: assumes db value are numerical, for now
    //TODO: use model instead of xml
    (tsml.xml \\ "time" \ "metadata" \ "@units").text match {
      case "" => ??? //no time units found
      case units: String => {
        //convert ISO time to units
        RegEx.TIME findFirstIn value match {
          case Some(s) => {
            val t = Time(javax.xml.bind.DatatypeConverter.parseDateTime(s).getTimeInMillis().toDouble)
            val t2 = t.convert(TimeScale(units)).getNumberData.doubleValue
            val tvname = (tsml.xml \\ "time" \ "@name").text //TODO: also look in metadata
            this.selections += tvname + op + t2
            true
          }
          case None => ??? //value didn't match time format
        }
      }
    }
  }
  
  //Override to apply projection to the model, scalars only, for now.
//TODO: keep original Dataset (e.g. so we can select on non-projected variables)
  //TODO: maintain projection order, this means modifying the model higher up
  //TODO: deal with composite names for nested vars
  override def makeScalar(sml: ScalarMl): Option[Scalar] = {
    //metadata/name complications make it easier to make the var first
    //TODO: filter before making
    super.makeScalar(sml) match {
      case os @ Some(s) => { //super class made a valid Scalar
        if (projection == "*") os
        //check if the Scalar has a matching name or alias
        else projection.split(",").find(s.hasName(_)) match {  
          case Some(_) => Some(s)  //found a match
          case None => None  //no match
        }
      }
      case _ => None  //superclass gave us None
    }
  }
  
  /*
   * TODO: clarify what happens before/after dataset is constructed vs accessed
   * need to make source dataset then "wrap"? it to apply projection to the model (and provenance) 
   * 
   */
  
  private lazy val resultSet: ResultSet = executeQuery
  private lazy val statement: Statement = connection.createStatement()

  private def executeQuery: ResultSet =  {
    val sql = makeQuery
    logger.debug("Executing sql query: " + sql)
    
    //Apply optional limit to the number of rows
    //TODO: Figure out how to warn the user if the limit is exceeded
    properties.get("limit") match {
      case Some(limit) => statement.setMaxRows(limit.toInt)
      case _ => 
    }
    
    //Apply FirstFilter or LastFilter. 
    //Set max rows to 1. "last" will set order to descending.
    //TODO: error if both set, unless there was only one
    if (first || last) statement.setMaxRows(1)
    
    statement.executeQuery(sql)
  }
  
  protected def makeQuery: String = {
    //TODO: sanitize stuff from properties, only in the data providers domain, but still...
    
    val sb = new StringBuffer()
    sb append "select "

    sb append projection
    
    sb append " from " + properties("table")
    
    val p = predicate 
    if (p.nonEmpty) sb append " where " + p
    
    //sort by the domain variable (e.g. time) 
    //TODO: generalize for n-D domains, get Function domain...
    //TODO: assuming that the first variable is the one to sort on
    //  make sure that tsml lists that variable first
    //Don't sort if domain is Index (orig domain not selected)
//TODO: what about when we want time order but not the time values? e.g. tlm packets
//  need to apply projection later? but only for domain?
    dataset.toSeq.head match {
      case _: Index => 
      case v: Variable => sb append " ORDER BY " + v.getName + order
    }
    
    sb.toString
  }
  
  /**
   * Build a list of constraints for the "where" clause.
   */
  lazy val predicate: String = {
    //start with selection clauses from requested operations
    val buffer = selections
    //TODO: just build on selections
    
      
    //add processing instructions
    //TODO: diff PI name? unfortunate that "filter" is more intuitive for a relational algebra "selection", "select"?
    //TODO: should PIs mutate the dataset? probably not, just like any other op, but the adapter's "dataset" should have them applied
    //buffer ++= tsml.getProcessingInstructions("filter")
    //PIs should be handled with other operations: select, project
    
    //get "predicate" if defined in the adapter attributes
    //TODO: need to be careful what we allow there, assumes selection clauses
    //  disallow for now since PIs can handle that
//    buffer += (properties.get("predicate") match {
//      case Some(s) => s
//      case None => ""
//    })
    
    //insert "AND" between the selection clauses
    buffer.filter(_.nonEmpty).mkString(" AND ")
  }
  
  //No query should be made until the iterator is called
  def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    //don't count Index vars
    //TODO: can we get away with flattening? consider nested Functions
    def recordSize = sampleTemplate.getSize
    //toSeq.filterNot(_.isInstanceOf[Index]).map(_.getSize).sum
    
    override def iterator = new NextIterator[Data] {
      val md = resultSet.getMetaData
      //val vars = dataset.toSeq //Seq of Variables as ordered in the dataset
      //TODO: maintain projection order, what if domain var is not first?
      
      val vars: Seq[Variable] = if (projection == "*") dataset.toSeq
      else projection.split(",").flatMap(dataset.getVariableByName(_))
      
      val types = vars.map(v => md.getColumnType(resultSet.findColumn(v.getName)))
      val varsWithTypes = vars zip types
      
      //Define a Calendar so we get our times in the GMT time zone.
      val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      
      //TODO: consider rs.getBinaryStream or getBytes
      
      def getNext: Data = {
        val bb = ByteBuffer.allocate(recordSize) 
        //TODO: reuse bb? but the previous sample is in the wild, memory resource issue, will gc help?
        if (resultSet.next) {
          for (vt <- varsWithTypes) vt match {
            case (v: Time, t: Int) if (t == java.sql.Types.TIMESTAMP) => {
              val time = resultSet.getTimestamp(v.getName, cal).getTime
              //TODO: support nanos?
              //deal with diff time types
              v match {
                case _: Real => bb.putDouble(time.toDouble)
                case _: Integer => bb.putLong(time)
              }
              //TODO: deal with text time type
              //TODO: should db with TIMESTAMP use "text" type?
              //Timestamp.toString => yyyy-mm-dd hh:mm:ss.fffffffff
            }
            //case (n: Number, _) => bb.putDouble(resultSet.getDouble(n.name))
            case (r: Real, _) => bb.putDouble(resultSet.getDouble(r.getName))
            case (i: Integer, _) => bb.putLong(resultSet.getLong(i.getName))
            case (t: Text, _) => {
              //pad the string to its full length using %ns formatting
              //TODO: regular words pad right, time strings from db pad left!?
              val s = "%"+t.length+"s" format resultSet.getString(t.getName)
              //fold each char into the ByteBuffer
              //println(t +": "+s)
              //TODO: make sure we don't exceed buffer
              s.foldLeft(bb)(_.putChar(_))
            }
            case (b: Binary, _) => bb.put(resultSet.getBytes(b.getName))  //TODO: use getBlob? 
            
            //TODO: error if column not found
          }
        
          Data(bb.flip.asInstanceOf[ByteBuffer]) //set limit and rewind so it is ready to be read
          
        } else null //no more samples
      }
    }
  }
  
  //---------------------------------------------------------------------------
    
  private lazy val connection: Connection = getConnection
    
  //hack so we don't end up getting a Connection when we are testing if we have one to close
  private var hasConnection = false 
  
  protected def getConnection: Connection = {
    hasConnection = true //TODO: what if getting connection fails
//TODO: use 'location' uri with 'jdbc' or 'java' (for jndi, e.g. java:comp/env/jdbc/sorce_l1a) scheme
    properties.get("jndi") match {
      case Some(jndi) => getConnectionViaJndi(jndi)
      case None => _getConnection
    }
  }
  
  private def getConnectionViaJndi(jndiName: String): Connection = {
    val initCtx = new InitialContext();
    val ds = initCtx.lookup(jndiName).asInstanceOf[DataSource];
    ds.getConnection();
  }
  
  private def _getConnection: Connection = {
    //TODO: error if not present
    val driver = properties("driver")
    val url = properties("location")
    val user = properties("user")
    val passwd = properties("password")
    
    //Load the JDBC driver 
    Class.forName(driver)

    //Make database connection
    DriverManager.getConnection(url, user, passwd)
  }
  
  
  def close() = {
    //Don't create the lazy connection just to close it.
    //TODO: do we need to close resultset, statement...?
    //  should we close it or just return it to the pool?
    //closing statement also closes resultset 
    //http://stackoverflow.com/questions/4507440/must-jdbc-resultsets-and-statements-be-closed-separately-although-the-connection
    if (hasConnection) {
      try { resultSet.close } catch {case e: Exception =>}
      try { statement.close } catch {case e: Exception =>}
      try { connection.close } catch {case e: Exception =>}
    }
  }
}