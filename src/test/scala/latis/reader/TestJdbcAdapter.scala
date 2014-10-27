package latis.reader

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import latis.ops.filter._
import latis.dm._
import latis.writer.Writer

class TestJdbcAdapter extends AdapterTests {
  def datasetName = "db"
    
  @Test
  def request_by_time {
    val ops = List(Selection("time>1970-01-02T00:00:00"))
    val data = getDataset(ops).toStringMap
    assertEquals("1970-01-03T00:00:00.000", data("myTime").head)
  }
  
  //@Test
  //TODO: Selection constructor removes white space
  def request_by_native_time {
    val ops = List(Selection("myTime>'1970-01-02 00:00:00'"))
    val data = getDataset(ops).toStringMap
    assertEquals("1970-01-03T00:00:00.000", data("myTime").head)
  }
  
  //TODO: move rename tests to AdapterTests once it supports general variables
  
   @Test
  def rename_range_variable {
    val ops = List(RenameOperation("myText", "theText"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("theText").length)
  }  
  
  @Test
  def rename_time_variable {
    val ops = List(RenameOperation("myTime", "theTime"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("theTime").length)
  }
  
  @Test
  def project_then_rename {
    val ops = List(Projection("myTime,myInt"), RenameOperation("myInt", "theInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)
    assertEquals(3, data("theInt").length)
  }
  
  @Test
  def rename_then_project {
    //projection likely needs to use the orig names for jdbc adapter? //TODO: make consistent with general op application
    val ops = List(RenameOperation("myTime", "theTime"), Projection("myTime,myText"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)
    assertEquals(3, data("theTime").length)
  }
  
  @Test
  def select_with_orig_name_then_rename {
    val ops = List(Selection("myInt>1"), RenameOperation("myInt", "theInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("theInt").length)
  }
  
  @Test
  def rename_then_select {
    //Note, Selection must use orig name for now
    val ops = List(RenameOperation("myInt", "theInt"), Selection("myInt>1"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("theInt").length)
  }
  
  @Test
  def select_by_native_time {
    //need a time var defined as an int
    val ops = List(Selection("myInt>1"))
    val ds = TsmlReader("db_with_int_time.tsml").getDataset(ops)
    val data = ds.toStringMap
    assertEquals(2, data("myInt").length)
  }
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
