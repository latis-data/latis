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

class JdbcAdapter(tsml: Tsml) extends IterativeAdapter(tsml) {
    
  //Handle the Projection and Selection Operation-s
  private var projection = "*"
  private val selections = ArrayBuffer[String]()
  
  override def handleOperation(op: Operation): Boolean = op match {
    case Projection(p) => {this.projection = p; true}
    case Selection(s) => {this.selections += s; true}
    /*
     * TODO: support aliases (e.g. "time")
     * 
     * resolveName? 
     *   implies getting full name
     * getSourceName? 
     * getShortName?
     *   but variable could have been renamed
     *   but the (immutable) Dataset this guy know about won't be renamed?
     *   or will processing instructions be applied mutably?
     * 
     */
    
 /*
  * TODO: time format for sql
  * dataset.findTimeVariable? 
  * numeric units: if query string matched TIME regex, convert iso to var's timeScale
  * datetime: what should tsml units be?
  *   will be converted to java time
  *   but we need to know if the db uses datatime so we can form sql
  *   units="ISO"?
  *   use "format", save units for numeric times, default to java
  *   
  */
 
    case _ => false
  }
  
  //Override to apply projection to the model, scalars only, for now.
  //TODO: deal with composite names for nested vars
  override def makeScalar(sml: ScalarMl): Option[Scalar] = {
    //TODO: filter before making, metadata/name complications
    super.makeScalar(sml) match {
      case os @ Some(s) => {
        if (projection != "*" && !projection.split(",").contains(s.name)) None  
        else os
      }
      case _ => None
    }
  }
  
  
  private lazy val resultSet: ResultSet = executeQuery

  private def executeQuery: ResultSet =  {
    val sql = makeQuery
    val statement = connection.createStatement() //(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    
    //Apply optional limit to the number of rows
    //TODO: Figure out how to warn the user if the limit is exceeded
    properties.get("limit") match {
      case Some(limit) => statement.setMaxRows(limit.toInt)
      case _ => 
    }
    
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
    sb append " ORDER BY " + dataset.toSeq.head.name + " ASC"
    
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
    //TODO: diff PI name? unfortunate that "filter" is more intuitive for a relational algebra "selection"
//TODO: should PIs mutate the dataset?
    buffer ++= tsml.getProcessingInstructions("filter")
    
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
    def recordSize = sampleTemplate.size
    
    override def iterator = new NextIterator[Data] {
      val md = resultSet.getMetaData
      val vars = dataset.toSeq //Seq of Variables as ordered in the dataset
/*
 * TODO: has the dataset been projected yet??? e.g. model or metadata munged
 * this should be the orig dataset
 * but we are taking responsibility for handling Selections and Projections
 * note, we are only access the dataset here so we can match name with db type
 */
      
      val types = vars.map(v => md.getColumnType(resultSet.findColumn(v.name)))
//TODO: how does this handle Projection? vars from orig dataset shouldn't be in result set
      val varsWithTypes = vars zip types
      
      //Define a Calendar so we get our times in the GMT time zone.
      val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      
      def getNext: Data = {
        val bb = ByteBuffer.allocate(recordSize) 
        //TODO: reuse bb? but the previous sample is in the wild, memory resource issue, will gc help?
        if (resultSet.next) {
          for (vt <- varsWithTypes) vt match {
            case (v: Time, t: Int) if (t == java.sql.Types.TIMESTAMP) => 
                  bb.putDouble(resultSet.getTimestamp(v.name, cal).getTime)
            //case (n: Number, _) => bb.putDouble(resultSet.getDouble(n.name))
            case (r: Real, _) => bb.putDouble(resultSet.getDouble(r.name))
            case (i: Integer, _) => {
              //println(resultSet.getLong(i.name))
              bb.putLong(resultSet.getLong(i.name))
            }
            //TODO: Text, series of chars? or convert to CharBuffer? require fixed length, default to 8 bytes (4 chars)
            //case (t: Text, _) => resultSet.getString(t.name).foldLeft(bb)(_.putChar(_))
            
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
    //TODO: support jndi
    val driver = properties("driver")
    val url = properties("url")
    val user = properties("user")
    val passwd = properties("password")
    
    //Load the JDBC driver 
    Class.forName(driver)

    //Make database connection
    DriverManager.getConnection(url, user, passwd)
  }
  
  
  def close() = {
    //Don't create the lazy connection just to close it.
    if (hasConnection) connection.close
  }
}