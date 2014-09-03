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
 * TODO: release connection as soon as possible?
 * risky leaving it open waiting for client to iterate
 * cache eagerly?
 * at least close after first iteration is complete, data in cache
 */

/**
 * Adapter for databases that support JDBC.
 */
class JdbcAdapter(tsml: Tsml) extends IterativeAdapter[JdbcAdapter.JdbcRecord](tsml) with Logging {
  //TODO: catch exceptions and close connections

  def getRecordIterator: Iterator[JdbcAdapter.JdbcRecord] = new JdbcAdapter.JdbcRecordIterator(resultSet)

  /**
   * Parse the data based on the Variable type (and the database column type, for time).
   */
  def parseRecord(record: JdbcAdapter.JdbcRecord): Option[Map[String, Data]] = {
    val map = mutable.Map[String, Data]()
    val rs = record.resultSet

    for (vt <- varsWithTypes) vt match {
      case (v: Time, dbtype: Int) if (dbtype == java.sql.Types.TIMESTAMP) => {
        //time stored as timestamp must be declared as type text
        var time = rs.getTimestamp(v.getName, gmtCalendar).getTime
        if (rs.wasNull) map += (v.getName -> Data(v.getFillValue.asInstanceOf[String]))
        else { //Get time format from Time variable units, default to ISO
          val s = v.getMetadata("units") match {
            case Some(units) => TimeFormat(units).format(time)
            case None => TimeFormat.ISO.format(time) //default to ISO yyyy-MM-ddTHH:mm:ss.SSS
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
   * Note, this will honor the order of the variables in the projection clause.
   */
  lazy val varsWithTypes = {
    //Get list of projected Scalars in projection order
    val vars: Seq[Variable] = if (projection == "*") getOrigScalars
    else projection.split(",").flatMap(getOrigDataset.findVariableByName(_))

    //Get the types of these variables in the database
    val md = resultSet.getMetaData
    val types = vars.map(v => md.getColumnType(resultSet.findColumn(v.getName)))

    //Combine the variables with their database types in a Seq of pairs.
    vars zip types
  }

  //Define a Calendar so we get our times in the default GMT time zone.
  private lazy val gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))

  //Handle the Projection and Selection Operation-s
  private var projection = "*"
  private val selections = ArrayBuffer[String]()

  //Handle first, last ops
  private var first = false
  private var last = false

  //Define sorting order.
  private var order = " ASC"

  /**
   * Handle the operations if we can so we can reduce the data volume at the source
   * so the parent adapter doesn't have to do as much.
   * Return true if this adapter is taking responsibility for applying the operation
   * or false if it won't.
   */
  override def handleOperation(operation: Operation): Boolean = operation match {
    case p: Projection => {
      //make sure these match variable names or aliases
      if (!p.names.forall(getOrigDataset.findVariableByName(_).nonEmpty))
        throw new Error("Not all variables are available for the projection: " + p)

      this.projection = p.toString
      true
    }

    case sel @ Selection(name, op, value) => getOrigDataset.findVariableByName(name) match {
      case Some(v) if (v.isInstanceOf[Time]) => handleTimeSelection(name, op, value)
      case Some(v) if (getOrigScalarNames.contains(name)) => {
        //add a selection to the sql, may need to change operation
        op match {
          case "==" =>
            selections append name + "=" + value; true
          case "=~" =>
            selections append name + " like '%" + value + "%'"; true
          case "~" => false //almost equal (e.g. nearest sample) not supported by sql
          case _ => selections append sel.toString; true
        }
      }
      case _ => {
        //Let LaTiS operations deal with constraints on index
        if (name == "index") false
        //This is not one of our variables. Err on the side of assuming this is a user mistake.
        else throw new Error("JdbcAdapter can't process selection for unknown parameter: " + name)
      }
    }

    case _: FirstFilter =>
      first = true; true
    case _: LastFilter =>
      last = true; order = " DESC"; true

    //TODO: handle exception, return false (not handled)?

    case _ => false //not an operation that we can handle
    //TODO: rename: select foo as bar?
  }

  /**
   * Special handling for a time selection since there are various formatting issues.
   */
  def handleTimeSelection(vname: String, op: String, value: String): Boolean = {
    //support ISO time string as value

    //Get the Time variable with the given name
    val tvar = getOrigDataset.findVariableByName(vname) match {
      case Some(t: Time) => t
      case _ => throw new Error("Time variable not found in dataset.")
    }
    val tvname = tvar.getName //original name which should match database column
    //TODO: consider implications of rename, apply after this? consider "origName"?

    tvar.getMetadata("type") match {
      case Some("text") => {
        //JDBC doesn't generally like the 'T' in the iso time.
        //Parse value into a Time then format consistent with java.sql.Timestamp.toString: yyyy-mm-dd hh:mm:ss.fffffffff
        //This should also treat the time as GMT. (Timestamp has internal Gregorian$Date which has the local time zone.)
        val time = Time.fromIso(value).format("yyyy-MM-dd HH:mm:ss.SSS")
        selections += tvname + op + "'" + time + "'"  //sql wants quotes around time value
        true
      }
      case _ => tvar.getMetadata("units") match {
        //case None => throw new Error("The dataset does not have time units defined, so you must use the native time: " + tvname)
        //TODO: we haven't established that these aren't native units
        case None => throw new Error("The dataset does not have time units defined for: " + tvname)
        //Note, The Time constructor will provide default units (ISO) if none are defined in the tsml.
        case Some(units) => {
          //convert ISO time selection value to dataset units
          try {
            val t = Time.fromIso(value).convert(TimeScale(units)).getValue
            this.selections += tvname + op + t
            true
          } catch {
            case iae: IllegalArgumentException => throw new Error("The time value is not in a supported ISO format: " + value)
            case e: Exception => throw new Error("Unable to parse time selection: " + value)
          }
        }
      }
    }
  }

  /**
   * Override to apply projection. Exclude Variables not listed in the projection.
   * Only works for Scalars, for now.
   */
  override def makeScalar(s: Scalar): Option[Scalar] = {
    //TODO: deal with composite names for nested vars
    if (projection == "*") super.makeScalar(s)
    else projection.split(",").find(s.hasName(_)) match { //account for aliases
      case Some(_) => super.makeScalar(s) //found a match
      case None => None //no match
    }
  }

  //---- Database Stuff -------------------------------------------------------

  /**
   * Execute the SQL query.
   */
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

  /**
   * Get the name of the database table from the adapter tsml attributes.
   */
  def getTable: String = getProperty("table") match {
    case Some(s) => s
    case None => throw new Error("JdbcAdapter needs to have a 'table' defined.")
  }

  /**
   * Construct the SQL query.
   */
  protected def makeQuery: String = getProperty("sql") match {
    //hack so we can define dataset with sql in the tsml file
    //TODO: revise handleOperation to return false for projection, selection,...
    case Some(sql) => sql
    case None => {
      //build query
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
          case i: Index => //implicit placeholder, use natural order
          case v: Variable => v match {
            case _: Scalar => sb append " ORDER BY " + v.getName + order
            case _ => ??? //TODO: generalize for n-D domains
          }
        }
        case None => //no function so no domain variable to sort by
      }

      sb.toString
    }
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
   * The JDBC ResultSet from the query. This will lazily execute the query
   * when this result is requested.
   */
  private lazy val resultSet: ResultSet = executeQuery
  private lazy val statement: Statement = connection.createStatement()
  //Keep database resources global so we can close them.

  /**
   * Allow subclasses to use the connection. They should not close it.
   */
  protected def getConnection: Connection = connection

  /*
   * Used so we don't end up getting the lazy connection when we are testing if we have one to close.
   */
  private var hasConnection = false

  /*
   * Database connection from JNDI or JDBC properties.
   */
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

  /**
   * Release the database resources.
   */
  override def close() = {
    //TODO: http://stackoverflow.com/questions/4507440/must-jdbc-resultsets-and-statements-be-closed-separately-although-the-connection
    if (hasConnection) {
      try { resultSet.close } catch { case e: Exception => }
      try { statement.close } catch { case e: Exception => }
      try { connection.close } catch { case e: Exception => }
    }
  }
}

//=============================================================================  

/**
 * Define some inner classes to provide us with Record semantics for JDBC ResultSets.
 */
object JdbcAdapter {

  case class JdbcRecord(resultSet: ResultSet)

  class JdbcRecordIterator(resultSet: ResultSet) extends Iterator[JdbcAdapter.JdbcRecord] {
    private var _didNext = false
    private var _hasNext = false

    def next() = {
      if (!_didNext) resultSet.next
      _didNext = false
      JdbcRecord(resultSet)
    }

    def hasNext() = {
      if (!_didNext) {
        _hasNext = resultSet.next
        _didNext = true
      }
      _hasNext
    }
  }
}
