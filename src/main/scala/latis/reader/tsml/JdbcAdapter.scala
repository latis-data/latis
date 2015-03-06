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
import latis.ops.RenameOperation
import latis.metadata.Metadata
import javax.naming.NameNotFoundException

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
    val map = varsWithTypes.map(vt => {
      val parse = (parseTime orElse parseReal orElse parseInteger orElse parseText orElse parseBinary)
      parse(vt).asInstanceOf[(String, Data)]
    }).toMap

    val sm = Some(map)
    sm
  }

  //TODO: might be nice to pass record to the PartialFunctions so we don't have to expose the ResultSet

  /**
   * Experiment with overriding just one case using PartialFunctions.
   * Couldn't just delegate to function due to the "if' guard.
   */
  protected val parseTime: PartialFunction[(Variable, Int), (String, Data)] = {
    //Note, need dbtype for filter but can't do anything outside the case
    case (v: Time, dbtype: Int) if (dbtype == java.sql.Types.TIMESTAMP) => {
      val name = getVariableName(v)
      val gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT")) //TODO: cache so we don't have to call for each sample?
      var time = resultSet.getTimestamp(name, gmtCalendar).getTime
      val s = if (resultSet.wasNull) v.getFillValue.asInstanceOf[String]
      else TimeFormat.ISO.format(time) //default to ISO yyyy-MM-ddTHH:mm:ss.SSS
      (name, Data(s))
    }
  }

  protected val parseReal: PartialFunction[(Variable, Int), (String, Data)] = {
    case (v: Real, _) => {
      val name = getVariableName(v)
      var d = resultSet.getDouble(name)
      if (resultSet.wasNull) d = v.getFillValue.asInstanceOf[Double]
      (name, Data(d))
    }
  }

  protected val parseInteger: PartialFunction[(Variable, Int), (String, Data)] = {
    case (v: Integer, _) => {
      val name = getVariableName(v)
      var l = resultSet.getLong(name)
      if (resultSet.wasNull) l = v.getFillValue.asInstanceOf[Long]
      (name, Data(l))
    }
  }

  protected val parseText: PartialFunction[(Variable, Int), (String, Data)] = {
    case (v: Text, _) => {
      val name = getVariableName(v)
      var s = resultSet.getString(name)
      if (resultSet.wasNull) s = v.getFillValue.asInstanceOf[String]
      s = StringUtils.padOrTruncate(s, v.length) //fix length as defined in tsml, default to 4
      (name, Data(s))
    }
  }

  protected val parseBinary: PartialFunction[(Variable, Int), (String, Data)] = {
    case (v: Binary, _) => {
      val name = getVariableName(v)
      (name, Data(resultSet.getBytes(name)))
    }
  }

  /**
   * Pairs of projected Variables (Scalars) and their database types.
   * Note, this will honor the order of the variables in the projection clause.
   */
  lazy val varsWithTypes = {
    //Get list of projected Scalars in projection order paired with their database type.
    //Saves us having to get the type for every sample.
    //Note, uses original variable names which are replaced for a rename operation as needed.
    val vars: Seq[Variable] = if (projectedVariableNames.isEmpty) getOrigScalars
    else projectedVariableNames.flatMap(getOrigDataset.findVariableByName(_)) //TODO: error if not found? redundant with other (earlier?) test

    //TODO: Consider case where PI does rename. User should never see orig names so should be able to use new name.

    //Get the types of these variables in the database.
    //Note, ResultSet columns should have new names from rename.
    val md = resultSet.getMetaData
    val types = vars.map(v => md.getColumnType(resultSet.findColumn(getVariableName(v))))

    //Combine the variables with their database types in a Seq of pairs.
    vars zip types
  }

  //Handle the Projection and Selection Operation-s
  //keep seq of names instead  //private var projection = "*"
  private var projectedVariableNames = Seq[String]()
  protected def getProjectedVariableNames = if (projectedVariableNames.isEmpty) getOrigScalarNames else projectedVariableNames

  protected val selections = ArrayBuffer[String]()

  //Keep map to store Rename operations until they are needed when constructing the sql.
  private val renameMap = mutable.Map[String, String]()

  /**
   * Use this to get the name of a Variable so we can apply rename.
   */
  protected def getVariableName(v: Variable): String = renameMap.get(v.getName) match {
    case Some(newName) => newName
    case None => v.getName
  }

  //Handle first, last ops
  private var first = false
  private var last = false

  //Define sorting order.
  private var order = "ASC"

  /**
   * Handle the operations if we can so we can reduce the data volume at the source
   * so the parent adapter doesn't have to do as much.
   * Return true if this adapter is taking responsibility for applying the operation
   * or false if it won't.
   */
  override def handleOperation(operation: Operation): Boolean = operation match {
    //TODO: should the handleFoo delegates be responsible for the true/false?
    case p: Projection => handleProjection(p)

    //TODO: factor out handleSelection?
    case sel @ Selection(name, op, value) => getOrigDataset.findVariableByName(name) match {
      //TODO: allow use of renamed variable? but sql where wants orig name
      case Some(v) if (v.isInstanceOf[Time]) => handleTimeSelection(name, op, value)
      case Some(v) if (getOrigScalarNames.contains(name)) => {
        //add a selection to the sql, may need to change operation
        op match {
          case "==" => v match {
            case _: Text => selections append name + "=" + quoteStringValue(value); true
            case _       => selections append name + "=" + value; true
          }
          case "=~" =>
            selections append name + " like '%" + value + "%'"; true
          case "~" => false //almost equal (e.g. nearest sample) not supported by sql
          case _ => v match {
            case _: Text => selections append name + op + quoteStringValue(value); true
            case _       => selections append name + op + value; true
          }
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
      last = true; order = "DESC"; true

    //Rename operation: apply in projection clause of sql: 'select origName as newName'
    //These will be combined with the projected variables in the select clause with "old as new".
    case RenameOperation(origName, newName) => {
      renameMap += (origName -> newName)
      true
    }

    //TODO: handle exception, return false (not handled)?

    case _ => false //not an operation that we can handle
  }

  /**
   * Make sure the given string is surrounded in quotes.
   */
  private def quoteStringValue(s: String): String = {
    //don't add if it is already quoted
    "'" + s.replaceAll("""^['"]""","").replaceAll("""['"]$""","") + "'"
  }
  
  /**
   * Handle a Projection clause.
   */
  def handleProjection(projection: Projection): Boolean = projection match {
    case p @ Projection(names) => {
      //make sure these match variable names or aliases
      if (!names.forall(getOrigDataset.findVariableByName(_).nonEmpty))
        throw new Error("Not all variables are available for the projection: " + p)
      projectedVariableNames = names
      true
    }
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
    val tvname = getVariableName(tvar)

    tvar.getMetadata("type") match {
      case Some("text") => {
        //A JDBC dataset with time defined as text implies the times are represented as a Timestamp.
        //JDBC doesn't generally like the 'T' in the iso time. (e.g. Derby)
        //Parse value into a Time then format consistent with java.sql.Timestamp.toString: yyyy-mm-dd hh:mm:ss.fffffffff
        //This should also treat the time as GMT. (Timestamp has internal Gregorian$Date which has the local time zone.)
        val time = Time.fromIso(value).format("yyyy-MM-dd HH:mm:ss.SSS")
        selections += tvname + op + "'" + time + "'" //sql wants quotes around time value
        true
      }
      case _ => {
        //So, we have a numeric time variable but need to figure out if the selection value is
        //  a numeric time (in native units) or an ISO time that needs to be converted.
        if (StringUtils.isNumeric(value)) {
          this.selections += tvname + op + value
          true
        } else tvar.getMetadata("units") match {
          //Assumes selection value is an ISO 8601 formatted string
          case None => throw new Error("The dataset does not have time units defined for: " + tvname)
          case Some(units) => {
            //convert ISO time selection value to dataset units
            //TODO: generalize for all unit conversions
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
  }

  /**
   * Override to apply projection. Exclude Variables not listed in the projection.
   * Only works for Scalars, for now.
   * If the rename operation needs to be applied, a new temporary Scalar will be created
   * with a copy of the original's metadata with the 'name' changed.
   */
  override def makeScalar(s: Scalar): Option[Scalar] = {
    //TODO: deal with composite names for nested vars
    getProjectedVariableNames.find(s.hasName(_)) match { //account for aliases
      case Some(_) => { //projected, see if it needs to be renamed
        val tmpScalar = renameMap.get(s.getName) match {
          case Some(newName) => s.updatedMetadata("name" -> newName)
          case None => s
        }
        super.makeScalar(tmpScalar)
      }
      case None => None //not projected
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
   * Build the select clause.
   * If no projection operation was provided, include all
   * since the tsml might expose only some database columns.
   * Apply rename operations.
   */
  protected def makeProjectionClause: String = {
    getProjectedVariableNames.map(name => {
      //If renamed, replace 'name' with 'name as "name2"'.
      //Use quotes so we can use reserved words like "min" (needed by Sybase).
      renameMap.get(name) match {
        case Some(name2) => name + """ as """" + name2 + """""""
        case None => name
      }
    }).mkString(",")
  }

  /**
   * Construct the SQL query.
   * Look for "sql" defined in the tsml, otherwise construct it.
   */
  protected def makeQuery: String = getProperty("sql") match {
    case Some(sql) => sql
    case None => {
      //build query
      val sb = new StringBuffer("select ")
      sb append makeProjectionClause
      sb append " from " + getTable

      val p = makePredicate
      if (p.nonEmpty) sb append " where " + p

      //Sort by domain variable.
      //assume domain is scalar, for now
      //Note 'dataset' should be the original before ops
      getOrigDataset.findFunction match {
        case Some(f) => f.getDomain match {
          case i: Index => //implicit placeholder, use natural order
          case v: Variable => v match {
            //Note, shouldn't matter if we sort on original name
            case _: Scalar => sb append " ORDER BY " + v.getName + " " + order
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
  protected def makePredicate: String = predicate
  private lazy val predicate: String = {
    //Get selection clauses (e.g. from requested operations)
    //Prepend any tsml defined predicate.
    val clauses = getProperty("predicate") match {
      case Some(s) => s +=: selections
      case None => selections
    }

    //insert "AND" between the clauses
    clauses.filter(_.nonEmpty).mkString(" AND ")
  }

  //---------------------------------------------------------------------------

  /**
   * The JDBC ResultSet from the query. This will lazily execute the query
   * when this result is requested.
   */
  protected lazy val resultSet: ResultSet = executeQuery
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
    val initCtx = new InitialContext()
    var ds: DataSource = null

    try {
      ds = initCtx.lookup(jndiName).asInstanceOf[DataSource]
    } catch {
      case e: NameNotFoundException => throw new Error("JdbcAdapter failed to locate JNDI resource: " + jndiName)
    }

    ds.getConnection()
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
