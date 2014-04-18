package latis.reader.tsml

import latis.data._
import latis.dm._
import java.sql.{ Connection, DriverManager, ResultSet }
import java.nio.ByteBuffer
import javax.naming.Context
import javax.naming.InitialContext
import javax.sql.DataSource
import latis.util.PeekIterator
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
import latis.time.TimeFormat
import java.util.Date
import java.sql.Timestamp
import latis.util.StringUtils
import latis.ops.filter.Selection
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter

class JdbcAdapter(tsml: Tsml) extends IterativeAdapter(tsml) with Logging {

  /*
   * TODO: 2014-02-24
   * deal with non-projected domain or range, replace with Index
   * let Projection do this on first pass? instead of doing the logic here?
   * but even projection delegated to projectedFunction (so it can set index value)
   * needs to be applied at sample level so you know if domain or range is empty
   * 
   */

  //TODO: catch exceptions and close connections

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

  //Note, no query should be made until the iterator is called.
  override def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    def recordSize = sampleTemplate.getSize

    override def iterator = new PeekIterator[Data] {
      //Use index to replace a non-projected domain. 
      var index = sampleTemplate.domain match {
        case _: Index => 0
        case _ => -1
      }

      //Get list of projected Scalars in projection order
      val vars: Seq[Variable] = if (projection == "*") getOrigDataset.toSeq
      else projection.split(",").flatMap(getOrigDataset.getVariableByName(_))

      //Get the types of these variables in the database
      val md = resultSet.getMetaData
      val types = vars.map(v => md.getColumnType(resultSet.findColumn(v.getName)))

      //Combine the variables with their database types in a Seq of pairs.
      //TODO: cleaner way? only needed for Time stored as TIMESTAMP?
      val varsWithTypes = vars zip types

      //Define a Calendar so we get our times in the GMT time zone.
      val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

      //TODO: consider rs.getBinaryStream or getBytes

      def getNext: Data = {
        val bb = ByteBuffer.allocate(recordSize)
        //TODO: reuse bb? but the previous sample is in the wild, memory resource issue, will gc help?


        
        if (resultSet.next) {
          
          /*
           * TODO: deal with nested functions
           * one outer record will be multiple rows
           * do something akin to Seq.grouped
           * 
           * 
           */
          
          //Add index value if domain not projected. Set to 0 above if we have an Index domain, -1 otherwise.
          //TODO: getNextIndex?
          if (index >= 0) {
            bb.putInt(index)
            index += 1
          }
          
          //TODO: handle null database cells
          //but for numeric values? (http://mods-jira.lasp.colorado.edu:8080/browse/LATIS-69)

          for (vt <- varsWithTypes) vt match {
            case (v: Time, t: Int) if (t == java.sql.Types.TIMESTAMP) => {
              //TODO: other database time types?
              val time = resultSet.getTimestamp(v.getName, cal).getTime
              //TODO: deal with missing: resultSet.wasNull
              //TODO: support nanos?
              //deal with diff time types
              v match {
                case _: Real => bb.putDouble(time.toDouble)
                case _: Integer => bb.putLong(time)
                case _: Text => {
                  //use units from Time variable
                  val s = v.getMetadata("units") match {
                    case Some(units) => TimeFormat(units).format(time)
                    case None => TimeFormat.ISO.format(time) //default to ISO yyyy-MM-ddTHH:mm:ss.SSS
                    //TODO: currently requires setting length="23" in tsml
                  }
                  s.foldLeft(bb)(_.putChar(_))
                }
              }
            }

            case (r: Real, _) => {
              var d = resultSet.getDouble(r.getName)
              if (resultSet.wasNull) d = r.getFillValue.asInstanceOf[Double]
              bb.putDouble(d)
            }
            
            case (i: Integer, _) =>{
              var l = resultSet.getLong(i.getName)
              if (resultSet.wasNull) l = i.getFillValue.asInstanceOf[Long]
              bb.putLong(l)
            } 
            
            case (t: Text, _) => {
              var s = resultSet.getString(t.getName)
              if (resultSet.wasNull) s = t.getFillValue.asInstanceOf[String]
              StringUtils.padOrTruncate(s, t.length).foldLeft(bb)(_.putChar(_)) //fix length, fold each char into the ByteBuffer
            }
            
            case (b: Binary, _) => bb.put(resultSet.getBytes(b.getName)) //TODO: use getBlob? deal with null?
          }

          Data(bb.flip.asInstanceOf[ByteBuffer]) //set limit and rewind so it is ready to be read

        } else null //no more samples
      }
    }
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