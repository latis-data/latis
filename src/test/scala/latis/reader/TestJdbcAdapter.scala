package latis.reader

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import latis.dm._
import latis.writer.Writer

class TestJdbcAdapter extends AdapterTests {
  def datasetName = "db"
}

object TestJdbcAdapter {
  
  //private var connection: Connection = null
  
  @BeforeClass
  def makeDatabase {
    //TODO: make sure these data are consistent with data used in other tests, ingest from same ascii file?
    //TODO: add Text type with length longer than default (4)

    System.setProperty("derby.stream.error.file", "/dev/null") //don't make log file
    
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")  //Load the JDBC driver     
    val connection = DriverManager.getConnection("jdbc:derby:memory:testDB;create=true")
    
    var statement = connection.createStatement()
    statement.execute("""create table test(myTime timestamp, myInt int, myReal double, myText varchar(1))""")
    statement.execute("insert into test values('1970-01-01 00:00:00', 1, 1.1, 'A')")
    statement.execute("insert into test values('1970-01-02 00:00:00', 2, 2.2, 'B')")
    statement.execute("insert into test values('1970-01-03 00:00:00', 3, 3.3, 'C')")
    
//    statement = connection.createStatement()
//    val rs = statement.executeQuery("select * from test where time > '1970-01-01T00:00:00'") 
//    //TODO: derby only works with '1970-01-01 00:00:00' !?  based on java.sql.Timestamp
//    while (rs.next()) {
//      println(rs.getString(1) + " " + rs.getInt(2) + " " + rs.getDouble(3) + " " + rs.getString(4))
//    }
  }
  
  @AfterClass
  def dropDatabase {
    try {
      DriverManager.getConnection("jdbc:derby:memory:testDB;drop=true")
    } catch {
      case e: Exception =>
    }
  }
}