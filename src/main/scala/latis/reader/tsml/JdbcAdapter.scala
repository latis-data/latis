package latis.reader.tsml

import latis.data.Data
import latis.data.IterableData
import latis.dm.Binary
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Variable
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.ops.filter.Selection
import latis.reader.tsml.ml.Tsml
import latis.time.Time
import latis.time.TimeFormat
import latis.time.TimeScale
import latis.util.PeekIterator
import latis.util.RegEx
import latis.util.RegEx.SELECTION
import latis.util.StringUtils
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.Calendar
import java.util.TimeZone
import scala.Array.fallbackCanBuildFrom
import scala.Option.option2Iterable
import scala.collection.Map
import scala.collection.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.slf4j.Logging
import javax.naming.InitialContext
import javax.sql.DataSource
import java.sql.Statement

/*
 * TODO: consider other APIs
 * slick: typesafe, but oracle api closed
 * anorm: play, might work
 *   JNDI: http://www.playframework.com/documentation/2.0/ScalaDatabaseOthers
 */
 
/* 
 * TODO: release connection as soon as possible?
 * risky leaving it open waiting for client to iterate
 * cache eagerly?
 * at least close after first iteration is complete, data in cache
 */

/**
 * Define some inner classes to provide us with Record semantics for JDBC ResultSets.
 */
object JdbcAdapter {
  
  case class JdbcRecord(resultSet: ResultSet) 
  
  class JdbcRecordIterator(resultSet: ResultSet) extends Iterator[JdbcAdapter.JdbcRecord] {
    private var _didNext = false
    private var _hasNext = false
    
    def next() = {
      if (! _didNext) resultSet.next
      _didNext = false
      JdbcRecord(resultSet)
    }
    
    def hasNext() = {
      if (! _didNext) {
        _hasNext = resultSet.next
        _didNext = true
      }
      _hasNext
    }
  }
}

class JdbcAdapter(tsml: Tsml) extends IterativeAdapter[JdbcAdapter.JdbcRecord](tsml) with Logging {
  //TODO: catch exceptions and close connections

  /*
   * TODO: refactor with Record semantics
   * make an internal class for Record
   * encapsulate ResultSet
   */

  def getRecordIterator: Iterator[JdbcAdapter.JdbcRecord] = new JdbcAdapter.JdbcRecordIterator(resultSet)

  def parseRecord(record: JdbcAdapter.JdbcRecord): Option[Map[String,Data]] = {
    val map = mutable.Map[String,Data]()
    val rs = record.resultSet
    
    for (vt <- varsWithTypes) vt match {
      case (v: Time, t: Int) if (t == java.sql.Types.TIMESTAMP) => {
        //time stored as timestamp must be declared as type text
        var time = rs.getTimestamp(v.getName, gmtCalendar).getTime
        if (rs.wasNull) map += (v.getName -> Data(v.getFillValue.asInstanceOf[String]))
        else { //Get time format from Time variable units, default to ISO
          val s = v.getMetadata("units") match {
            case Some(units) => TimeFormat(units).format(time)
            case None => TimeFormat.ISO.format(time) //default to ISO yyyy-MM-ddTHH:mm:ss.SSS
//TODO: currently requires setting length="23" in tsml
          }
          map += (v.getName -> Data(s))
        }
      }

      case (r: Real, _) => {
        var d = rs.getDouble(r.getName)
        if (rs.wasNull) d = r.getFillValue.asInstanceOf[Double]
        map += (r.getName -> Data(d))
      }

      case (i: Integer, _) => {
        var l = rs.getLong(i.getName)
        if (rs.wasNull) l = i.getFillValue.asInstanceOf[Long]
        map += (i.getName -> Data(l))
      }

      case (t: Text, _) => {
        var s = rs.getString(t.getName)
        if (rs.wasNull) s = t.getFillValue.asInstanceOf[String]
        s = StringUtils.padOrTruncate(s, t.length) //fix length as defined in tsml, default to 4
        map += (t.getName -> Data(s))
      }

      case (b: Binary, _) => map += (b.getName -> Data(rs.getBytes(b.getName))) //TODO: use getBlob? deal with null?
    }
    
    Some(map)
  }
  
  /**
   * Pairs of projected Variables (Scalars) and their database types.
   */
  lazy val varsWithTypes = {
    //Get list of projected Scalars in projection order
    val vars: Seq[Variable] = if (projection == "*") getOrigScalars
    else projection.split(",").flatMap(getOrigDataset.getVariableByName(_))

    //Get the types of these variables in the database
    val md = resultSet.getMetaData
    val types = vars.map(v => md.getColumnType(resultSet.findColumn(v.getName)))

    //Combine the variables with their database types in a Seq of pairs.
    vars zip types
  }
  
  //Define a Calendar so we get our times in the GMT time zone.
  private lazy val gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
  

  //Keep these global so we can close them.
  private lazy val resultSet: ResultSet = executeQuery
  private lazy val statement: Statement = connection.createStatement()

  //Handle the Projection and Selection Operation-s
  private var projection = "*"
  private val selections = ArrayBuffer[String]()

  //Handle first, last ops
  private var first = false
  private var last = false

  //Define sorting order.
  private var order = " ASC"

  override def handleOperation(operation: Operation): Boolean = operation match {
    case p: Projection => {
      //make sure these match variable names or aliases
      if (!p.names.forall(getOrigDataset.getVariableByName(_).nonEmpty))
        throw new Error("Not all variables are available for the projection: " + p)

      this.projection = p.toString
      true
    }

    case sel: Selection =>
      val expression = sel.toString; expression match { //TODO: can we use an alias with '@'...?
        //Break expression up into components
        case SELECTION.r(name, op, value) => {
          if (name == "time") handleTimeSelection(op, value) //special handling for "time"
          //TODO: handle other Time variables (dependent)
          else if (getOrigScalarNames.contains(name)) { //other variable (not time), even if not projected
            //TODO: quote text values, or expect selection to be that way?
            //we may want to do that for the same reason sql does: value could be a variable as opposed to a literal
            
            
            //add a selection to the sql, may need to change operation
            op match {
              case "==" =>
                selections append name + "=" + value; true
              case "=~" =>
                selections append name + " like '%" + value + "%'"; true
              case "~" => false //almost equal (e.g. nearest sample) not supported by sql
              case _ => selections append expression; true //TODO: sanitize, but already matched Selection regex
            }
          } else false //doesn't apply to our variables, so leave it for the next handler, TODO: or error?
        }
        //TODO: case _ => doesn't match selection regex, error
      }

    case _: FirstFilter =>
      first = true; true
    case _: LastFilter =>
      last = true; order = " DESC"; true

    //TODO: handle exception, return false (not handled)?

    case _ => false //not an operation that we can handle
    //TODO: rename: select foo as bar?
  }

  def handleTimeSelection(op: String, value: String): Boolean = {
    //support ISO time string as value
    //TODO: assumes value is ISO, what if dataset does have a var named "time" with other units?

    //this should work because any time variable should have the alias "time"
    val tvar = getOrigDataset.getVariableByName("time") match {
      case Some(t) => t
      case None => throw new Error("No time variable found in dataset.")
    }
    val tvname = tvar.getName //original name which should match database column

    tvar.getMetadata("type") match {
      case Some("text") => {
        //JDBC doesn't generally like the 'T' in the iso time.
        //Parse value into a Time then format consistent with java.sql.Timestamp.toString: yyyy-mm-dd hh:mm:ss.fffffffff
        //This should also treat the time as GMT. (Timestamp has internal Gregorian$Date which has the local time zone.)
        val time = Time.fromIso(value).format("yyyy-MM-dd HH:mm:ss.SSS")
        selections += tvname + op + "'" + time + "'"
        true
      }
      case _ => tvar.getMetadata("units") match {
        case None => throw new Error("The dataset does not have time units defined, so you must use the native time: " + tvname)
        //Note, The Time constructor will provide default units (ISO) if none are defined in the tsml.
        case Some(units) => {
          //convert ISO time selection value to dataset units
          //regex ensures that the value is a valid ISO time
          RegEx.TIME.r findFirstIn value match {
            case Some(s) => {
              val t = Time.fromIso(s).convert(TimeScale(units)).getValue
              this.selections += tvname + op + t
              true
            }
            case None => throw new Error("The time value is not in a supported ISO format: " + value)
          }
        }
      }
    }
  }

  //Override to apply projection to the model, scalars only, for now.
  //TODO: maintain projection order, this means modifying the model higher up, or when building Sample?
  //TODO: deal with composite names for nested vars
  override def makeScalar(s: Scalar): Option[Scalar] = {
    if (projection == "*") super.makeScalar(s)
    else projection.split(",").find(s.hasName(_)) match { //account for aliases
      case Some(_) => {
        super.makeScalar(s) //found a match
      }
      case None => None //no match
    }
  }

  private def executeQuery: ResultSet = {
    val sql = makeQuery
    logger.debug("Executing sql query: " + sql)

    //Apply optional limit to the number of rows
    //TODO: Figure out how to warn the user if the limit is exceeded
    getProperty("limit") match {
      case Some(limit) => statement.setMaxRows(limit.toInt)
      case _ =>
    }

    //Allow specification of number of rows to fetch at a time.
    getProperty("fetchSize") match {
      case Some(fetchSize) => statement.setFetchSize(fetchSize.toInt)
      case _ =>
    }

    //Apply FirstFilter or LastFilter. 
    //Set max rows to 1. "last" will set order to descending.
    //TODO: error if both set, unless there was only one
    if (first || last) statement.setMaxRows(1)

    statement.executeQuery(sql)
  }

  def getTable: String = getProperty("table") match {
    case Some(s) => s
    case None => throw new Error("JdbcAdapter needs to have a 'table' defined.")
  }

  protected def makeQuery: String = {
    //TODO: sanitize stuff from properties, only in the data providers domain, but still...

    val sb = new StringBuffer()
    sb append "select "

    sb append projection

    sb append " from " + getTable

    val p = predicate
    if (p.nonEmpty) sb append " where " + p

    //Sort by domain variable.
    //assume domain is scalar, for now
    //Note 'dataset' should be the original before ops
    //val dvar = findDomainVariable(dataset) 
    getOrigDataset.findFunction match {
      case Some(f) => f.getDomain match {
        case i: Index => //use natural order
        case v: Variable => v match {
          case _: Scalar => sb append " ORDER BY " + v.getName + order
          case _ => ??? //TODO: generalize for n-D domains, Function in domain?
        }
        case _ => ??? //TODO: error? Function has no domain
      }
      case None => //no function so domain variable to sort by
    }

    sb.toString
  }

  /**
   * Build a list of constraints for the "where" clause.
   */
  lazy val predicate: String = {
    //start with selection clauses from requested operations
    val buffer = selections

    //insert "AND" between the selection clauses
    buffer.filter(_.nonEmpty).mkString(" AND ")
  }

  
  //---------------------------------------------------------------------------

  /**
   * Allow subclasses to use the connection. They should not close it.
   */
  protected def getConnection: Connection = connection

  //Used so we don't end up getting the lazy connection when we are testing if we have one to close
  private var hasConnection = false

  private lazy val connection: Connection = {
    //TODO: use 'location' uri for jndi with 'java' (e.g. java:comp/env/jdbc/sorce_l1a) scheme, but glassfish doesn't use that form
    val con = getProperty("jndi") match {
      case Some(jndi) => getConnectionViaJndi(jndi)
      case None => getConnectionViaJdbc
    }

    hasConnection = true //will still be false if getting connection fails
    con
  }

  private def getConnectionViaJndi(jndiName: String): Connection = {
    val initCtx = new InitialContext();
    val ds = initCtx.lookup(jndiName).asInstanceOf[DataSource];
    ds.getConnection();
  }

  private def getConnectionViaJdbc: Connection = {
    val driver = getProperty("driver") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a JDBC 'driver' defined.")
    }
    val url = getProperty("location") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a JDBC 'url' defined.")
    }
    val user = getProperty("user") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a 'user' defined.")
    }
    val passwd = getProperty("password") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a 'password' defined.")
    }

    //Load the JDBC driver 
    Class.forName(driver)

    //Make database connection
    DriverManager.getConnection(url, user, passwd)
  }

  override def close() = {
    //TODO: do we need to close resultset, statement...?
    //  should we close it or just return it to the pool?
    //closing statement also closes resultset 
    //http://stackoverflow.com/questions/4507440/must-jdbc-resultsets-and-statements-be-closed-separately-although-the-connection
    if (hasConnection) {
      try { resultSet.close } catch { case e: Exception => }
      try { statement.close } catch { case e: Exception => }
      try { connection.close } catch { case e: Exception => }
    }
  }
}